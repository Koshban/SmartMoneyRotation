"""
src/ingest_options.py
Fetch option chains and save to parquet + CSV.

Sources:
  yfinance  — US options (IV, bid/ask, volume, OI — no greeks)
  IBKR TWS  — US/HK options (full greeks, real-time)

Auto-selects:
  market == "hk"  →  IBKR  (yfinance has no HK options)
  Otherwise       →  yfinance

HK Options Notes:
  - HKEX stock options expire on the penultimate business day
    of the month (NOT the 3rd Friday like US).
  - The IBKR fetcher bypasses pre-calculated target dates for HK
    and picks expiries directly from the available chain, which
    is robust regardless of holiday calendar.
  - IBKR HK option contracts use exchange "SEHK" and "HKD".
  - HK stock codes have leading zeros stripped for IBKR
    (e.g. 0700.HK → "700", 9988.HK → "9988").

Usage:
    python src/ingest_options.py --market us                    # yfinance
    python src/ingest_options.py --market us --source ibkr      # IBKR
    python src/ingest_options.py --market hk                    # IBKR (auto)
    python src/ingest_options.py --market hk --tickers 9988.HK  # single HK
    python src/ingest_options.py --market us --rungs 7
    python src/ingest_options.py --market us --consolidate
"""
import sys
from pathlib import Path

_SRC  = Path(__file__).resolve().parent
_ROOT = _SRC.parent
for _p in (str(_ROOT), str(_SRC)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import argparse
import logging
import time
from datetime import date
import pandas as pd
import yfinance as yf

from common.universe import (
    get_us_only_etfs,
    get_all_single_names,
    get_hk_only,
    is_hk_ticker,
    is_india_ticker,
)
from common.expiry import (
    next_monthly_expiries,
    match_expiry,
    select_expiries_from_chain,
)

try:
    from common.credentials import IBKR_PORT, IBKR_HOST
except ImportError:
    IBKR_PORT = 7497
    IBKR_HOST = "127.0.0.1"

try:
    from common.credentials import IBKR_CLIENT_ID_INGEST
except ImportError:
    IBKR_CLIENT_ID_INGEST = 10

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)-20s %(levelname)-10s %(message)s",
)
LOG = logging.getLogger(__name__)

DATA_DIR = _ROOT / "data"
RUNGS    = 5
DELAY_YF = 1.5
DELAY_IBKR = 0.6

# ── Dedup key for options (module-level constant) ─────────────
_OPTIONS_DEDUP_KEYS = ["date", "symbol", "expiry", "strike", "opt_type"]

# ═══════════════════════════════════════════════════════════════
#  SYMBOL LISTS
# ═══════════════════════════════════════════════════════════════

def get_symbols(market: str) -> list[str]:
    if market == "us":
        etfs = get_us_only_etfs()
        singles = [
            s for s in get_all_single_names()
            if not is_hk_ticker(s) and not is_india_ticker(s)
        ]
        return sorted(set(etfs + singles))
    elif market == "hk":
        return get_hk_only()
    else:
        LOG.warning(f"Options not supported for market: {market}")
        return []


def choose_source(market: str, force_source: str | None = None) -> str:
    if force_source:
        return force_source
    if market == "hk":
        return "ibkr"
    return "yfinance"


# ═══════════════════════════════════════════════════════════════
#  YFINANCE FETCH
# ═══════════════════════════════════════════════════════════════

def current_price_yf(ticker: yf.Ticker) -> float | None:
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


def fetch_symbol_yfinance(
    symbol: str,
    n_rungs: int = 5,
) -> pd.DataFrame | None:
    ticker = yf.Ticker(symbol)
    price = current_price_yf(ticker)
    if price is None:
        LOG.warning(f"    {symbol}: no price, skipping")
        return None

    try:
        available = ticker.options
    except Exception as e:
        LOG.warning(f"    {symbol}: cannot read expiries – {e}")
        return None

    if not available:
        LOG.warning(f"    {symbol}: no options listed")
        return None

    targets = next_monthly_expiries(market="us", n=2)
    matched = match_expiry(targets, available)

    if not matched:
        LOG.warning(f"    {symbol}: no monthly expiry matched")
        return None

    LOG.info(
        f"    {symbol:<10s}  price={price:>10.2f}   "
        f"expiries={[str(d) for d, _ in matched]}"
    )

    today = date.today()
    rows = []

    for exp_date, exp_str in matched:
        try:
            chain = ticker.option_chain(exp_str)
        except Exception as e:
            LOG.warning(f"    {symbol} {exp_str}: chain error – {e}")
            continue

        otm_p, otm_c = select_strikes(chain.puts, chain.calls, price, n_rungs)

        for _, r in otm_p.iterrows():
            rows.append(_yf_row(symbol, exp_date, r, "P", price, today))

        for _, r in otm_c.iterrows():
            rows.append(_yf_row(symbol, exp_date, r, "C", price, today))

        time.sleep(DELAY_YF)

    return pd.DataFrame(rows) if rows else None


def _yf_row(symbol, exp_date, r, opt_type, price, today) -> dict:
    return {
        "date":             today.isoformat(),
        "symbol":           symbol,
        "expiry":           exp_date.isoformat(),
        "strike":           r["strike"],
        "opt_type":         opt_type,
        "bid":              r.get("bid"),
        "ask":              r.get("ask"),
        "last":             r.get("lastPrice"),
        "volume":           r.get("volume"),
        "oi":               r.get("openInterest"),
        "iv":               r.get("impliedVolatility"),
        "delta":            None,
        "gamma":            None,
        "theta":            None,
        "vega":             None,
        "rho":              None,
        "underlying_price": price,
        "source":           "yfinance",
    }


# ═══════════════════════════════════════════════════════════════
#  IBKR FETCH
# ═══════════════════════════════════════════════════════════════

def _make_stock_contract(symbol: str, market: str):
    """Build an ib_insync Stock contract."""
    from ib_insync import Stock

    if market == "hk":
        # IBKR uses numeric code without leading zeros for SEHK
        # 0700.HK → "700",  9988.HK → "9988",  0005.HK → "5"
        ibkr_sym = symbol.replace(".HK", "").lstrip("0") or "0"
        return Stock(ibkr_sym, "SEHK", "HKD")
    else:
        ibkr_sym = symbol.split(".")[0]
        return Stock(ibkr_sym, "SMART", "USD")


def _make_option_contract(
    symbol: str,
    expiry_str: str,
    strike: float,
    right: str,
    market: str,
):
    """Build an ib_insync Option contract."""
    from ib_insync import Option

    if market == "hk":
        ibkr_sym = symbol.replace(".HK", "").lstrip("0") or "0"
        return Option(
            ibkr_sym, expiry_str, strike, right,
            "SEHK", currency="HKD",
        )
    else:
        ibkr_sym = symbol.split(".")[0]
        return Option(
            ibkr_sym, expiry_str, strike, right,
            "SMART", currency="USD",
        )


def _ibkr_expiry_to_iso(expiry: str) -> str:
    """Convert IBKR '20250620' format to '2025-06-20'."""
    if len(expiry) == 8 and expiry.isdigit():
        return f"{expiry[:4]}-{expiry[4:6]}-{expiry[6:]}"
    return expiry


def _select_expiries_for_market(
    available_expiries: list[str],
    market: str,
) -> list[tuple[date, str]]:
    """
    Select the next 2 monthly expiries from IBKR's available list,
    using the appropriate method for each market.

    For HK: Directly pick from the chain (bypasses pre-calculated
            targets entirely — robust regardless of HKEX holidays).

    For US: Match against 3rd-Friday targets first.  If that fails
            (can happen around holidays), fall back to chain-based
            selection.

    Returns list of (expiry_date, ISO string) pairs.
    """
    iso_expiries = [_ibkr_expiry_to_iso(e) for e in available_expiries]

    if market == "hk":
        # ── HK: use chain-based selection ─────────────────────
        # HK expiry convention (penultimate business day) differs
        # from US (3rd Friday).  Pre-calculated targets often miss
        # because of holidays and the ~2 week offset.  The robust
        # approach is to pick directly from what IBKR says is
        # available.
        selected = select_expiries_from_chain(
            available_expiries, n=2,
        )
        if selected:
            return selected

        # Fallback: try matching with HK-specific targets and
        # a wider gap tolerance
        targets = next_monthly_expiries(market="hk", n=2)
        matched = match_expiry(targets, iso_expiries, max_gap_days=12)
        return matched

    else:
        # ── US: match against 3rd-Friday targets ──────────────
        targets = next_monthly_expiries(market="us", n=2)
        matched = match_expiry(targets, iso_expiries, max_gap_days=7)
        if matched:
            return matched

        # Fallback: direct chain-based selection
        LOG.debug(
            "US target matching failed — falling back to "
            "chain-based expiry selection"
        )
        return select_expiries_from_chain(
            available_expiries, n=2,
        )


def fetch_symbol_ibkr(
    ib,
    symbol: str,
    market: str,
    n_rungs: int = 5,
) -> pd.DataFrame | None:
    """
    Fetch options chain for one symbol via IBKR TWS.

    Returns DataFrame with full greeks.

    HK-specific handling:
      - Stock contract on SEHK in HKD
      - Expiry selection uses chain-based approach (not 3rd Friday)
      - Option contracts also on SEHK in HKD
    """

    # ── Get underlying price ──────────────────────────────────
    stock = _make_stock_contract(symbol, market)
    qualified = ib.qualifyContracts(stock)
    if not qualified:
        LOG.warning(f"    {symbol}: could not qualify stock contract")
        return None

    stock = qualified[0]

    ib.reqMarketDataType(3)  # delayed-frozen as fallback
    ticker_data = ib.reqMktData(stock, "", False, False)
    ib.sleep(2)

    price = ticker_data.marketPrice()
    if price is None or price != price:
        price = ticker_data.close
    if price is None or price != price or price <= 0:
        LOG.warning(f"    {symbol}: no price from IBKR")
        ib.cancelMktData(stock)
        return None

    ib.cancelMktData(stock)

    # ── Get available option chains ───────────────────────────
    try:
        chains = ib.reqSecDefOptParams(
            stock.symbol, "", stock.secType, stock.conId,
        )
    except Exception as e:
        LOG.warning(f"    {symbol}: reqSecDefOptParams failed – {e}")
        return None

    if not chains:
        LOG.warning(f"    {symbol}: no option chains available")
        return None

    # For HK, IBKR may return multiple chain objects.
    # Pick the one with the most strikes (usually the primary exchange).
    # For US, this is typically the SMART chain.
    chain = max(chains, key=lambda c: len(c.strikes))

    available_expiries = sorted(chain.expirations)
    available_strikes = sorted(chain.strikes)

    if not available_expiries:
        LOG.warning(f"    {symbol}: no expiries in chain")
        return None

    LOG.debug(
        f"    {symbol}: {len(available_expiries)} expiries, "
        f"{len(available_strikes)} strikes  "
        f"(exchange={chain.exchange}, "
        f"tradingClass={chain.tradingClass})"
    )

    # ── Select next 2 monthly expiries ────────────────────────
    matched = _select_expiries_for_market(
        available_expiries, market,
    )

    if not matched:
        LOG.warning(
            f"    {symbol}: no monthly expiry matched  "
            f"(available: {available_expiries[:6]}...)"
        )
        return None

    LOG.info(
        f"    {symbol:<10s}  price={price:>10.2f}   "
        f"expiries={[str(d) for d, _ in matched]}  "
        f"({len(available_strikes)} strikes)"
    )

    today = date.today()
    rows = []

    for exp_date, exp_str in matched:
        # Convert ISO to IBKR format (YYYYMMDD)
        ibkr_expiry = exp_str.replace("-", "")

        # Select nearest OTM strikes
        puts_strikes = sorted(
            [s for s in available_strikes if s <= price],
            reverse=True,
        )[:n_rungs]

        calls_strikes = sorted(
            [s for s in available_strikes if s >= price],
        )[:n_rungs]

        for strike in puts_strikes:
            row = _fetch_single_option(
                ib, symbol, market, ibkr_expiry, strike,
                "P", price, today, exp_date,
            )
            if row:
                rows.append(row)
            ib.sleep(DELAY_IBKR)

        for strike in calls_strikes:
            row = _fetch_single_option(
                ib, symbol, market, ibkr_expiry, strike,
                "C", price, today, exp_date,
            )
            if row:
                rows.append(row)
            ib.sleep(DELAY_IBKR)

    if not rows:
        return None

    df = pd.DataFrame(rows)

    # ── Deduplicate at source ──────────────────────────────────
    before = len(df)
    df = df.drop_duplicates(
        subset=_OPTIONS_DEDUP_KEYS, keep="last",
    ).reset_index(drop=True)
    dupes = before - len(df)
    if dupes:
        LOG.info(f"    {symbol}: dropped {dupes} duplicate option rows")

    return df if not df.empty else None


def _fetch_single_option(
    ib, symbol, market, ibkr_expiry, strike, right,
    underlying_price, today, exp_date,
) -> dict | None:
    """Fetch data for a single option contract from IBKR."""
    try:
        contract = _make_option_contract(
            symbol, ibkr_expiry, strike, right, market,
        )
        qualified = ib.qualifyContracts(contract)
        if not qualified:
            LOG.debug(
                f"    {symbol} {right}{strike} {ibkr_expiry}: "
                f"could not qualify"
            )
            return None

        contract = qualified[0]

        ticker = ib.reqMktData(contract, "106", False, False)
        ib.sleep(1.5)

        row = {
            "date":             today.isoformat(),
            "symbol":           symbol,
            "expiry":           exp_date.isoformat(),
            "strike":           strike,
            "opt_type":         right,
            "bid":              _safe_float(ticker.bid),
            "ask":              _safe_float(ticker.ask),
            "last":             _safe_float(ticker.last),
            "volume":           _safe_int(ticker.volume),
            "oi":               None,
            "iv":               _safe_float(
                ticker.modelGreeks.impliedVol
                if ticker.modelGreeks else None
            ),
            "delta":            _safe_float(
                ticker.modelGreeks.delta
                if ticker.modelGreeks else None
            ),
            "gamma":            _safe_float(
                ticker.modelGreeks.gamma
                if ticker.modelGreeks else None
            ),
            "theta":            _safe_float(
                ticker.modelGreeks.theta
                if ticker.modelGreeks else None
            ),
            "vega":             _safe_float(
                ticker.modelGreeks.vega
                if ticker.modelGreeks else None
            ),
            "rho":              _safe_float(
                ticker.modelGreeks.rho
                if ticker.modelGreeks else None
            ),
            "underlying_price": underlying_price,
            "source":           "ibkr",
        }

        ib.cancelMktData(contract)
        return row

    except Exception as e:
        LOG.debug(
            f"    {symbol} {right}{strike} {ibkr_expiry}: {e}"
        )
        return None


def _safe_float(val) -> float | None:
    if val is None:
        return None
    try:
        f = float(val)
        if f != f:
            return None
        return f
    except (TypeError, ValueError):
        return None


def _safe_int(val) -> int | None:
    if val is None:
        return None
    try:
        f = float(val)
        if f != f:
            return None
        return int(f)
    except (TypeError, ValueError):
        return None


# ═══════════════════════════════════════════════════════════════
#  IBKR SESSION MANAGER
# ═══════════════════════════════════════════════════════════════

def run_ibkr(symbols: list[str], market: str, n_rungs: int):
    try:
        from ib_insync import IB
    except ImportError:
        LOG.error("ib_insync not installed. Run: pip install ib_insync")
        return

    ib = IB()
    try:
        ib.connect(IBKR_HOST, IBKR_PORT, clientId=IBKR_CLIENT_ID_INGEST)
        LOG.info(f"[IBKR] Connected to TWS at {IBKR_HOST}:{IBKR_PORT}")
    except Exception as e:
        LOG.error(f"[IBKR] Cannot connect to TWS: {e}")
        if market == "hk":
            LOG.error("HK options require IBKR — no fallback available.")
            return
        LOG.info("Falling back to yfinance for US options (no greeks)")
        run_yfinance(symbols, n_rungs)
        return

    out_dir = DATA_DIR / "options" / market
    out_dir.mkdir(parents=True, exist_ok=True)

    total = 0
    errors = 0
    skips = 0

    try:
        for i, sym in enumerate(symbols, 1):
            LOG.info(f"[{i}/{len(symbols)}]  {sym}")

            try:
                df = fetch_symbol_ibkr(ib, sym, market, n_rungs)
            except Exception as e:
                LOG.error(f"    {sym}: unexpected error – {e}")
                errors += 1
                continue

            if df is None or df.empty:
                skips += 1
                continue

            fname = sym.replace(".", "_").replace("/", "_") + ".csv"
            path = out_dir / fname
            _append_save(df, path)

            day_ct = int((df["date"] == date.today().isoformat()).sum())
            total += day_ct
            LOG.info(f"         → {day_ct} contracts ({path.name})")

    finally:
        ib.disconnect()
        LOG.info("[IBKR] Disconnected")

    LOG.info(
        f"DONE {market.upper()} (IBKR): "
        f"{total} contracts | {skips} skipped | {errors} errors"
    )


# ═══════════════════════════════════════════════════════════════
#  YFINANCE SESSION RUNNER
# ═══════════════════════════════════════════════════════════════

def run_yfinance(symbols: list[str], n_rungs: int):
    out_dir = DATA_DIR / "options" / "us"
    out_dir.mkdir(parents=True, exist_ok=True)

    total = 0
    errors = 0
    skips = 0

    for i, sym in enumerate(symbols, 1):
        LOG.info(f"[{i}/{len(symbols)}]  {sym}")

        try:
            df = fetch_symbol_yfinance(sym, n_rungs)
        except Exception as e:
            LOG.error(f"    {sym}: unexpected error – {e}")
            errors += 1
            continue

        if df is None or df.empty:
            skips += 1
            continue

        fname = sym.replace(".", "_").replace("/", "_") + ".csv"
        path = out_dir / fname
        _append_save(df, path)

        day_ct = int((df["date"] == date.today().isoformat()).sum())
        total += day_ct
        LOG.info(f"         → {day_ct} contracts ({path.name})")

        time.sleep(DELAY_YF)

    LOG.info(
        f"DONE US (yfinance): "
        f"{total} contracts | {skips} skipped | {errors} errors"
    )


# ═══════════════════════════════════════════════════════════════
#  CONSOLIDATION
# ═══════════════════════════════════════════════════════════════

def consolidate(market: str = "us") -> pd.DataFrame:
    """Merge per-ticker CSVs into one parquet, deduped."""
    csv_dir = DATA_DIR / "options" / market
    if not csv_dir.exists():
        LOG.warning(f"No options directory: {csv_dir}")
        return pd.DataFrame()

    frames = []
    for csv_file in sorted(csv_dir.glob("*.csv")):
        try:
            df = pd.read_csv(csv_file, dtype={"date": str, "expiry": str})
            if not df.empty:
                frames.append(df)
        except Exception as e:
            LOG.warning(f"Failed to read {csv_file.name}: {e}")

    if not frames:
        LOG.warning(f"No CSV files found in {csv_dir}")
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)

    # ── Deduplicate across all tickers ─────────────────────────
    before = len(combined)
    combined = combined.drop_duplicates(
        subset=_OPTIONS_DEDUP_KEYS, keep="last",
    ).reset_index(drop=True)
    dupes = before - len(combined)
    if dupes:
        LOG.info(f"Consolidation dedup: removed {dupes:,} duplicate rows")

    out_path = DATA_DIR / f"{market}_options.parquet"
    combined.to_parquet(out_path, index=False)

    LOG.info(
        f"Consolidated {len(frames)} CSVs → {out_path.name}  "
        f"({len(combined):,} rows, "
        f"{combined['symbol'].nunique()} symbols)"
    )
    return combined


# ═══════════════════════════════════════════════════════════════
#  SHARED HELPERS
# ═══════════════════════════════════════════════════════════════

def _append_save(df: pd.DataFrame, path: Path):
    """
    Append today's data to a per-ticker CSV, deduplicating by the
    full composite key.  Safe to call multiple times on the same day.
    """
    if path.exists():
        prev = pd.read_csv(path, dtype={"date": str, "expiry": str})
        df = pd.concat([prev, df], ignore_index=True)

    before = len(df)
    df = df.drop_duplicates(subset=_OPTIONS_DEDUP_KEYS, keep="last")
    dupes = before - len(df)
    if dupes:
        LOG.debug(f"  {path.name}: removed {dupes} duplicate rows")

    df.to_csv(path, index=False)


# ═══════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Fetch options chains (yfinance + IBKR)",
    )
    parser.add_argument(
        "--market", choices=["us", "hk"], default="us",
        help="Market to fetch (default: us)",
    )
    parser.add_argument(
        "--source", choices=["yfinance", "ibkr"], default=None,
        help="Force data source (default: auto)",
    )
    parser.add_argument(
        "--rungs", type=int, default=RUNGS,
        help=f"OTM strikes per side (default: {RUNGS})",
    )
    parser.add_argument(
        "--consolidate", action="store_true",
        help="Consolidate per-ticker CSVs into one parquet",
    )
    parser.add_argument(
        "--tickers", nargs="+", default=None,
        help="Fetch specific tickers only",
    )
    args = parser.parse_args()

    if args.consolidate:
        consolidate(args.market)
        return

    if args.tickers:
        symbols = [t.upper() for t in args.tickers]
    else:
        symbols = get_symbols(args.market)

    if not symbols:
        LOG.error(f"No symbols for market: {args.market}")
        return

    source = choose_source(args.market, args.source)

    LOG.info("=" * 60)
    LOG.info(
        f"OPTIONS — {args.market.upper()} | "
        f"{len(symbols)} symbols | "
        f"{args.rungs} rungs/side | "
        f"source: {source}"
    )
    LOG.info("=" * 60)

    if source == "ibkr":
        run_ibkr(symbols, args.market, args.rungs)
    else:
        run_yfinance(symbols, args.rungs)

    LOG.info("=" * 60)
    LOG.info(f"Output → {DATA_DIR / 'options' / args.market}")
    LOG.info("=" * 60)


if __name__ == "__main__":
    main()