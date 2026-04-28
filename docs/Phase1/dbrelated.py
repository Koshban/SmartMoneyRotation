"""
src/ingest_cash.py – Download OHLCV universe data (yfinance + IBKR).

Auto-selects data source:
  Period ≤ 5 days   → IBKR TWS  (must be running)
  Period > 5 days   → yfinance  (bulk backfill)

Override with --source yfinance | ibkr

Outputs:
  data/{market}_cash.parquet   — per-market files (for load_db.py)
  data/universe_ohlcv.parquet  — combined file   (for loader.py)

Usage:
    python src/ingest_cash.py --market all --period 2y
    python src/ingest_cash.py --market all --days 180
    python src/ingest_cash.py --market us  --days 365
    python src/ingest_cash.py --market all --period 3d
    python src/ingest_cash.py --market us --period 5d --source ibkr
    python src/ingest_cash.py --full --backfill
"""

import sys
from pathlib import Path
_SRC  = Path(__file__).resolve().parent        # .../src
_ROOT = _SRC.parent                            # .../SmartMoneyRotation
for _p in (str(_ROOT), str(_SRC)):
    if _p not in sys.path:
        sys.path.insert(0, _p)
import math
import logging
import argparse
import re
from datetime import datetime, date, timedelta

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pandas as pd
import yfinance as yf

from common.credentials import IBKR_PORT
from common.universe import (
    get_us_only_etfs,
    get_all_single_names,
    get_hk_only,
    get_india_only,
    is_hk_ticker,
    is_india_ticker,
)

try:
    from common.credentials import IBKR_HOST
except ImportError:
    IBKR_HOST = "127.0.0.1"

try:
    from common.credentials import IBKR_CLIENT_ID_INGEST
except ImportError:
    IBKR_CLIENT_ID_INGEST = 10

# ── Paths ──────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)

# ── Period → approximate calendar days ─────────────────────────
PERIOD_DAYS_MAP = {
    "1d": 1, "2d": 2, "3d": 3, "4d": 4, "5d": 5,
    "1w": 7, "2w": 14, "1mo": 30, "3mo": 90, "6mo": 180,
    "1y": 365, "2y": 730, "5y": 1825, "10y": 3650, "max": 9999,
}

IBKR_THRESHOLD_DAYS = 5
IBKR_SUPPORTED_MARKETS = {"us", "hk"}

# ── Rolling parquet window ─────────────────────────────────────
# 450 calendar days ≈ 310 trading days — enough for EMA(200) +
# convergence runway.  Data older than this is trimmed on save.
MAX_PARQUET_CALENDAR_DAYS = 450


# ====================================================================
#  Symbol lists from universe.py
# ====================================================================

def get_symbols_for_market(market: str) -> list[str]:
    """Build symbol list for a market using universe.py helpers."""
    if market == "us":
        etfs = get_us_only_etfs()
        singles = [
            s for s in get_all_single_names()
            if not is_hk_ticker(s) and not is_india_ticker(s)
        ]
        combined = list(dict.fromkeys(etfs + singles))
        return combined

    elif market == "hk":
        return get_hk_only()

    elif market == "in":
        return get_india_only()

    else:
        logger.warning(f"Unknown market: {market}")
        return []


# ====================================================================
#  Helpers
# ====================================================================

def period_to_days(period: str) -> int:
    """Convert a period string like '2y', '5d', '3mo' to approx calendar days."""
    period = period.lower().strip()
    if period in PERIOD_DAYS_MAP:
        return PERIOD_DAYS_MAP[period]

    m = re.match(r"^(\d+)\s*(d|w|mo|m|y)$", period)
    if m:
        n = int(m.group(1))
        unit = m.group(2)
        if unit == "d":
            return n
        if unit == "w":
            return n * 7
        if unit in ("mo", "m"):
            return n * 30
        if unit == "y":
            return n * 365

    logger.warning(f"Cannot parse period '{period}', defaulting to 9999 days (yfinance)")
    return 9999


def days_to_ibkr_duration(days: int) -> str:
    """Convert a calendar-day count to IBKR durationStr format."""
    if days <= 7:
        return f"{days} D"
    elif days <= 60:
        weeks = max(1, days // 7)
        return f"{weeks} W"
    elif days <= 365:
        months = max(1, days // 30)
        return f"{months} M"
    else:
        years = max(1, days // 365)
        return f"{years} Y"


def period_to_ibkr_duration(period: str) -> str:
    """Convert period string to IBKR durationStr format like '5 D'."""
    days = period_to_days(period)
    return days_to_ibkr_duration(days)


def choose_source(period: str = None, market: str = "us",
                  force_source: str = None, days: int = None) -> str:
    """
    Decide whether to use 'yfinance' or 'ibkr'.
      1. If force_source is set, use that.
      2. If period ≤ 5 days AND market is IBKR-supported → ibkr
      3. Otherwise → yfinance
    """
    if force_source:
        return force_source

    effective_days = days if days is not None else period_to_days(period or "2y")
    if effective_days <= IBKR_THRESHOLD_DAYS and market in IBKR_SUPPORTED_MARKETS:
        return "ibkr"

    return "yfinance"


def normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise column names to lowercase for load_db.py compatibility.
    """
    df = df.copy()
    df.columns = [str(c).lower().strip() for c in df.columns]

    renames = {
        "adj close": "adj_close",
    }
    df.rename(columns={k: v for k, v in renames.items() if k in df.columns},
              inplace=True)

    return df


# ====================================================================
#  Cumulative parquet upsert
# ====================================================================

def upsert_parquet(
    new_df: pd.DataFrame,
    parquet_path: Path,
    date_col: str = "date",
    symbol_col: str = "symbol",
    max_calendar_days: int = MAX_PARQUET_CALENDAR_DAYS,
) -> pd.DataFrame:
    """
    Append *new_df* to the existing parquet at *parquet_path*,
    deduplicate by (symbol, date), trim to rolling window,
    and overwrite the file.

    This turns a simple daily fetch into a cumulative local store
    with enough history for long-lookback indicators (EMA-200, etc.).

    Parameters
    ----------
    new_df : pd.DataFrame
        Fresh rows from today's fetch.
    parquet_path : Path
        Where the cumulative parquet lives.
    date_col / symbol_col : str
        Column names used for dedup and trimming.
    max_calendar_days : int
        How far back to keep.  450 calendar days ≈ 310 trading
        days — enough for EMA(200) + convergence runway.

    Returns
    -------
    pd.DataFrame
        The merged, deduplicated, trimmed DataFrame (also saved to disk).
    """
    parquet_path = Path(parquet_path)

    # ── Load existing history ─────────────────────────────────
    if parquet_path.exists():
        existing = pd.read_parquet(parquet_path)
        logger.info(
            f"  Existing parquet: {len(existing):,} rows, "
            f"{existing[symbol_col].nunique()} symbols, "
            f"{existing[date_col].min()} → {existing[date_col].max()}"
        )
    else:
        existing = pd.DataFrame()
        logger.info(f"  No existing parquet at {parquet_path.name} — starting fresh")

    # ── Coerce dates ──────────────────────────────────────────
    new_df = new_df.copy()
    new_df[date_col] = pd.to_datetime(new_df[date_col], errors="coerce")
    if not existing.empty:
        existing[date_col] = pd.to_datetime(existing[date_col], errors="coerce")

    # ── Concatenate ───────────────────────────────────────────
    combined = pd.concat([existing, new_df], ignore_index=True)

    # ── Deduplicate: keep LAST (newest fetch wins) ────────────
    before = len(combined)
    combined = combined.drop_duplicates(
        subset=[symbol_col, date_col],
        keep="last",
    )
    dupes = before - len(combined)
    if dupes:
        logger.info(f"  Parquet dedup: removed {dupes:,} duplicate rows")

    # ── Trim to rolling window ────────────────────────────────
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=max_calendar_days)
    before_trim = len(combined)
    combined = combined[combined[date_col] >= cutoff].copy()
    trimmed = before_trim - len(combined)
    if trimmed:
        logger.info(f"  Parquet trim: removed {trimmed:,} rows older than {cutoff.date()}")

    # ── Sort and save ─────────────────────────────────────────
    combined = combined.sort_values([symbol_col, date_col]).reset_index(drop=True)
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(parquet_path, index=False)

    logger.info(
        f"  Saved {parquet_path.name}: {len(combined):,} rows, "
        f"{combined[symbol_col].nunique()} symbols, "
        f"{combined[date_col].min().date()} → {combined[date_col].max().date()}"
    )

    return combined


# ====================================================================
#  IBKR contract helpers
# ====================================================================

def clean_hk_symbol(symbol: str) -> str:
    """'0005.HK' → '5', '0700.HK' → '700'"""
    sym = symbol.replace(".HK", "").lstrip("0")
    return sym if sym else "0"


def make_ibkr_contract(symbol: str, market: str):
    """Build an ib_insync Stock contract from a yfinance-style symbol."""
    from ib_insync import Stock

    if market == "hk":
        ibkr_sym = clean_hk_symbol(symbol)
        return Stock(ibkr_sym, "SEHK", "HKD")
    elif market == "in":
        ibkr_sym = symbol.replace(".NS", "").replace(".BO", "")
        return Stock(ibkr_sym, "NSE", "INR")
    else:
        ibkr_sym = symbol.split(".")[0]
        return Stock(ibkr_sym, "SMART", "USD")


# ====================================================================
#  yfinance fetch
# ====================================================================

def fetch_yfinance(
    symbols: list[str],
    period: str = "2y",
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    if start_date and end_date:
        logger.info(
            f"[yfinance] Downloading {len(symbols)} symbols, "
            f"{start_date} → {end_date}"
        )
        df = yf.download(
            tickers=symbols,
            start=start_date,
            end=end_date,
            interval="1d",
            group_by="ticker",
            auto_adjust=False,
            threads=True,
        )
    else:
        logger.info(
            f"[yfinance] Downloading {len(symbols)} symbols, "
            f"period={period}"
        )
        df = yf.download(
            tickers=symbols,
            period=period,
            interval="1d",
            group_by="ticker",
            auto_adjust=False,
            threads=True,
        )

    if df.empty:
        logger.warning("[yfinance] Empty result")
        return pd.DataFrame()

    records = []

    if len(symbols) == 1:
        sym = symbols[0]
        tmp = df.copy()
        tmp = tmp.reset_index()
        tmp["symbol"] = sym
        records.append(tmp)
    else:
        for sym in symbols:
            try:
                tmp = df[sym].copy()
                tmp = tmp.dropna(how="all")
                if tmp.empty:
                    continue
                tmp = tmp.reset_index()
                tmp["symbol"] = sym
                records.append(tmp)
            except KeyError:
                logger.warning(f"[yfinance] No data for {sym}")
                continue

    if not records:
        return pd.DataFrame()

    result = pd.concat(records, ignore_index=True)

    col_map = {}
    for c in result.columns:
        if c.lower() == "date":
            col_map[c] = "Date"
    result.rename(columns=col_map, inplace=True)

    # ── Deduplicate at source ──────────────────────────────────
    before = len(result)
    result = result.drop_duplicates(subset=["Date", "symbol"], keep="last")
    dupes = before - len(result)
    if dupes:
        logger.info(f"[yfinance] Dropped {dupes:,} duplicate (Date, symbol) rows")

    logger.info(
        f"[yfinance] Got {len(result):,} rows for "
        f"{result['symbol'].nunique()} symbols"
    )
    return result


# ====================================================================
#  IBKR fetch
# ====================================================================

def fetch_ibkr(
    symbols: list[str],
    period: str = "5d",
    market: str = "us",
    days: int | None = None,
) -> pd.DataFrame:
    """
    Fetch historical daily bars from IBKR TWS.

    If *days* is given it is converted directly to an IBKR
    duration string, bypassing the period-string parsing.
    """
    try:
        from ib_insync import IB, util
    except ImportError:
        logger.error("ib_insync not installed. Run: pip install ib_insync")
        return pd.DataFrame()

    if days is not None:
        duration = days_to_ibkr_duration(days)
    else:
        duration = period_to_ibkr_duration(period)

    logger.info(
        f"[IBKR] Fetching {len(symbols)} symbols, "
        f"duration={duration}, market={market}"
    )

    ib = IB()
    try:
        ib.connect(IBKR_HOST, IBKR_PORT, clientId=IBKR_CLIENT_ID_INGEST)
        ib.reqMarketDataType(1)
        logger.info(f"[IBKR] Connected to TWS at {IBKR_HOST}:{IBKR_PORT}")
    except Exception as e:
        logger.error(f"[IBKR] Cannot connect to TWS: {e}")
        logger.warning("[IBKR] Falling back to yfinance")
        if days is not None:
            end_dt = datetime.now()
            start_dt = end_dt - timedelta(days=days)
            return fetch_yfinance(
                symbols,
                start_date=start_dt.strftime("%Y-%m-%d"),
                end_date=end_dt.strftime("%Y-%m-%d"),
            )
        return fetch_yfinance(symbols, period)

    all_records = []

    try:
        for idx, sym in enumerate(symbols, 1):
            logger.info(f"[IBKR] [{idx}/{len(symbols)}] {sym}")

            contract = make_ibkr_contract(sym, market)
            qualified = ib.qualifyContracts(contract)

            if not qualified:
                logger.warning(f"[IBKR]   Could not qualify {sym}, skipping")
                continue

            contract = qualified[0]

            try:
                bars = ib.reqHistoricalData(
                    contract,
                    endDateTime="",
                    durationStr=duration,
                    barSizeSetting="1 day",
                    whatToShow="TRADES",
                    useRTH=True,
                    formatDate=1,
                )
            except Exception as e:
                logger.warning(f"[IBKR]   Error fetching {sym}: {e}")
                continue

            if not bars:
                logger.warning(f"[IBKR]   No bars for {sym}")
                continue

            df = util.df(bars)
            df["symbol"] = sym

            df.rename(columns={
                "date":     "Date",
                "open":     "Open",
                "high":     "High",
                "low":      "Low",
                "close":    "Close",
                "volume":   "Volume",
                "average":  "VWAP",
                "barCount": "Trades",
            }, inplace=True)

            df["Adj Close"] = df["Close"]

            all_records.append(df)
            logger.info(f"[IBKR]   {sym}: {len(df)} bars")

            ib.sleep(0.5)

    finally:
        ib.disconnect()
        logger.info("[IBKR] Disconnected")

    if not all_records:
        return pd.DataFrame()

    result = pd.concat(all_records, ignore_index=True)

    # ── Deduplicate at source ──────────────────────────────────
    before = len(result)
    result = result.drop_duplicates(subset=["Date", "symbol"], keep="last")
    dupes = before - len(result)
    if dupes:
        logger.info(f"[IBKR] Dropped {dupes:,} duplicate (Date, symbol) rows")

    logger.info(
        f"[IBKR] Got {len(result):,} rows for "
        f"{result['symbol'].nunique()} symbols"
    )
    return result


# ====================================================================
#  Orchestration
# ====================================================================

def fetch_full_universe(
    markets: list[str],
    period: str = "2y",
    days: int | None = None,
    force_source: str = None,
):
    DATA_DIR.mkdir(exist_ok=True)
    all_dfs = []

    start_date: str | None = None
    end_date: str | None = None
    if days is not None:
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=days)
        start_date = start_dt.strftime("%Y-%m-%d")
        end_date = end_dt.strftime("%Y-%m-%d")

    for market in markets:
        symbols = get_symbols_for_market(market)
        if not symbols:
            logger.warning(f"No symbols for market: {market}")
            continue

        source = choose_source(
            period=period, market=market,
            force_source=force_source, days=days,
        )

        effective_label = (
            f"{days} days" if days is not None else f"period={period}"
        )
        logger.info(
            f"Market: {market.upper()} | "
            f"{len(symbols)} symbols | "
            f"{effective_label} | "
            f"source: {source}"
        )

        if source == "ibkr":
            df = fetch_ibkr(symbols, period, market, days=days)
        else:
            df = fetch_yfinance(
                symbols,
                period=period,
                start_date=start_date,
                end_date=end_date,
            )

        if df is None or df.empty:
            logger.warning(f"No data returned for market: {market}")
            continue

        # ── Normalise columns to lowercase ─────────────────────
        df = normalise_columns(df)

        # ── Deduplicate by (date, symbol) before saving ────────
        before = len(df)
        df = df.drop_duplicates(subset=["date", "symbol"], keep="last")
        df = df.reset_index(drop=True)
        dupes = before - len(df)
        if dupes:
            logger.info(f"Deduped {market}: removed {dupes:,} rows")

        # ── Save per-market parquet (CUMULATIVE upsert) ────────
        market_path = DATA_DIR / f"{market}_cash.parquet"
        merged = upsert_parquet(
            new_df=df,
            parquet_path=market_path,
            date_col="date",
            symbol_col="symbol",
        )

        all_dfs.append(merged)

    if not all_dfs:
        logger.warning("No data collected across any market")
        return

    combined = pd.concat(all_dfs, ignore_index=True)

    # ── Final cross-market dedup (safety net) ──────────────────
    combined = combined.drop_duplicates(
        subset=["date", "symbol"], keep="last",
    ).reset_index(drop=True)

    combined_path = DATA_DIR / "universe_ohlcv.parquet"
    combined.to_parquet(combined_path, index=False)
    size_mb = combined_path.stat().st_size / (1024 * 1024)
    logger.info(
        f"Saved → {combined_path}  "
        f"({size_mb:.1f} MB, {len(combined):,} rows, "
        f"{combined['symbol'].nunique()} symbols — combined)"
    )


# ====================================================================
#  CLI
# ====================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Ingest OHLCV data (yfinance + IBKR)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  python src/ingest_cash.py --market all --period 2y        # 2-year backfill
  python src/ingest_cash.py --market us  --days 365         # US, exactly 365 days
  python src/ingest_cash.py --market all --days 180         # all markets, 180 days
  python src/ingest_cash.py --market us  --period 5d --source ibkr
  python src/ingest_cash.py --full --backfill
        """,
    )
    parser.add_argument(
        "--market",
        choices=["us", "hk", "in", "all"],
        default="all",
        help="Which market(s) to download (default: all)",
    )
    parser.add_argument(
        "--period",
        default="2y",
        help="Period: 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, max (default: 2y)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=None,
        help=(
            "Calendar days of data to download (overrides --period). "
            "E.g. --days 365 downloads the last 365 calendar days."
        ),
    )
    parser.add_argument(
        "--source",
        choices=["yfinance", "ibkr"],
        default=None,
        help="Force data source (default: auto based on period/days)",
    )
    parser.add_argument(
        "--full", action="store_true",
        help="Alias for --market all",
    )
    parser.add_argument(
        "--backfill", action="store_true",
        help="Alias for --period max",
    )
    args = parser.parse_args()

    if args.full:
        args.market = "all"
    if args.backfill:
        args.period = "max"
        args.days = None       # --backfill wins over --days

    if args.market == "all":
        markets = ["us", "hk", "in"]
    else:
        markets = [args.market]

    # ── Resolve effective days for display ─────────────────────
    if args.days is not None:
        effective_days = args.days
        logger.info(f"--days {args.days} → downloading {effective_days} calendar days")
    else:
        effective_days = period_to_days(args.period)
        logger.info(
            f"Period={args.period} ({effective_days} days) → "
            f"auto threshold: ≤{IBKR_THRESHOLD_DAYS}d uses IBKR"
            + (f" [OVERRIDDEN → {args.source}]" if args.source else "")
        )

    # Show symbol counts
    for mkt in markets:
        syms = get_symbols_for_market(mkt)
        src = choose_source(
            period=args.period, market=mkt,
            force_source=args.source, days=args.days,
        )
        logger.info(f"  {mkt.upper():6s}: {len(syms):>4d} symbols → {src}")

    fetch_full_universe(
        markets=markets,
        period=args.period,
        days=args.days,
        force_source=args.source,
    )

    logger.info("Done")


if __name__ == "__main__":
    main()

#####################################
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

###########################################
"""
src/db/loader.py
--------------
Unified OHLCV data loader for the CASH compute pipeline.

Reads from:
  1. PostgreSQL regional cash tables (canonical, accumulates via upsert)
  2. Local parquet files (cumulative cache — fallback if DB unavailable)
  3. yfinance (last-resort fallback for missing tickers)

Returns DataFrames in the standard format expected by compute/:
  - Columns: open, high, low, close, volume
  - DatetimeIndex named "date", sorted ascending
  - No NaN/zero closes

The optional ``days`` parameter on every public function limits
output to the most recent N calendar days.  Data sources that
support it (DB, yfinance) use server-side filtering for speed;
parquet data is filtered after reading from cache.
"""
from __future__ import annotations
import sys
from pathlib import Path
from datetime import datetime, timedelta

_SRC  = Path(__file__).resolve().parent.parent  # .../src
_ROOT = _SRC.parent                             # .../SmartMoneyRotation
for _p in (str(_ROOT), str(_SRC)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import numpy as np
import pandas as pd
from common.config import DATA_DIR
import logging
logger = logging.getLogger(__name__)

# ── Parquet paths ─────────────────────────────────────────────
_UNIVERSE_PARQUET = DATA_DIR / "universe_ohlcv.parquet"
_INDIA_PARQUET    = DATA_DIR / "in_cash.parquet"

_REQUIRED_COLS = ["open", "high", "low", "close", "volume"]

# ── Module-level cache ────────────────────────────────────────
_parquet_cache: dict[Path, pd.DataFrame] = {}

# ── SQLAlchemy engine cache (one engine per unique PG_CONFIG) ─
_sa_engine = None

# ── Minimum history thresholds ────────────────────────────────
MIN_BARS_HARD  = 60    # pipeline refuses to run below this
MIN_BARS_WARN  = 200   # warning: long-lookback indicators unreliable


# ═══════════════════════════════════════════════════════════════
#  SQLALCHEMY ENGINE HELPER
# ═══════════════════════════════════════════════════════════════

def _get_engine():
    """
    Build (and cache) a SQLAlchemy engine from PG_CONFIG.

    Uses the ``postgresql+psycopg2`` dialect so psycopg2 stays
    the underlying driver — the only change is that pandas
    receives a proper SQLAlchemy connectable instead of a raw
    DBAPI2 connection.
    """
    global _sa_engine
    if _sa_engine is not None:
        return _sa_engine

    from sqlalchemy import create_engine
    from common.credentials import PG_CONFIG

    user = PG_CONFIG.get("user", "")
    password = PG_CONFIG.get("password", "")
    host = PG_CONFIG.get("host", "localhost")
    port = PG_CONFIG.get("port", 5432)
    dbname = PG_CONFIG.get("dbname", PG_CONFIG.get("database", ""))

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"

    _sa_engine = create_engine(url, pool_pre_ping=True)
    logger.info("Created SQLAlchemy engine for PostgreSQL")
    return _sa_engine


# ═══════════════════════════════════════════════════════════════
#  DAYS FILTER HELPER
# ═══════════════════════════════════════════════════════════════

def _apply_days_filter(
    df: pd.DataFrame,
    days: int | None,
) -> pd.DataFrame:
    """
    Trim a DatetimeIndex-ed DataFrame to the most recent *days*
    calendar days.  Returns *df* unchanged if ``days`` is None
    or the frame is empty.
    """
    if days is None or df.empty:
        return df
    cutoff = pd.Timestamp.now().normalize() - pd.Timedelta(days=days)
    filtered = df[df.index >= cutoff]
    return filtered


def _days_to_yf_period(days: int) -> str:
    """
    Convert a calendar-day count to the closest yfinance period
    string that is guaranteed to cover at least ``days``.

    yfinance only accepts: 1d 5d 1mo 3mo 6mo 1y 2y 5y 10y max
    """
    if days <= 5:
        return "5d"
    if days <= 30:
        return "1mo"
    if days <= 90:
        return "3mo"
    if days <= 180:
        return "6mo"
    if days <= 365:
        return "1y"
    if days <= 730:
        return "2y"
    if days <= 1825:
        return "5y"
    if days <= 3650:
        return "10y"
    return "max"


# ═══════════════════════════════════════════════════════════════
#  MINIMUM HISTORY CHECK
# ═══════════════════════════════════════════════════════════════

def check_minimum_history(
    universe_frames: dict[str, pd.DataFrame],
    min_bars: int = MIN_BARS_HARD,
    warn_bars: int = MIN_BARS_WARN,
) -> None:
    """
    Verify the loaded universe has enough history for indicators.

    Call this in runner_v2 right after loading data and before
    computing any indicators.

    Raises
    ------
    ValueError
        If the median ticker has fewer than *min_bars* rows.
        The error message tells the user to backfill.

    Logs a warning if history is between *min_bars* and *warn_bars*.
    """
    if not universe_frames:
        raise ValueError(
            "Universe is empty — no ticker data loaded. "
            "Run ingest_cash.py first."
        )

    lengths = [len(df) for df in universe_frames.values()]
    median_len = sorted(lengths)[len(lengths) // 2]
    min_len = min(lengths)

    if median_len < min_bars:
        raise ValueError(
            f"Insufficient history: median ticker has {median_len} bars, "
            f"need at least {min_bars} for indicators to compute. "
            f"Run: python src/ingest_cash.py --market <mkt> --period 2y  "
            f"to backfill, then: python src/db/load_db.py --type cash"
        )

    if median_len < warn_bars:
        logger.warning(
            "Limited history: median ticker has %d bars (ideal >= %d). "
            "EMA(200) and long-lookback indicators may be unreliable.",
            median_len, warn_bars,
        )
    else:
        logger.info(
            "History check OK: median=%d bars, min=%d bars, tickers=%d",
            median_len, min_len, len(universe_frames),
        )


# ═══════════════════════════════════════════════════════════════
#  PUBLIC API
# ═══════════════════════════════════════════════════════════════

def load_ohlcv(
    ticker: str,
    source: str = "auto",
    days: int | None = None,
) -> pd.DataFrame:
    """
    Load OHLCV for a single ticker.

    Parameters
    ----------
    ticker : str
        Symbol, e.g. "AAPL", "XLK", "RELIANCE.NS", "2800.HK".
    source : str
        "parquet" — local parquet files only
        "db"      — PostgreSQL only
        "yfinance"— yfinance download
        "auto"    — try db → parquet → yfinance
    days : int, optional
        If given, only return the most recent *days* calendar days
        of data.  For DB and yfinance sources the filter is applied
        server-side; for parquet it is applied after reading from
        cache.

    Returns
    -------
    pd.DataFrame
        Columns: open, high, low, close, volume.
        DatetimeIndex sorted ascending.
        Empty DataFrame if loading fails.
    """
    if source == "auto":
        # Try DB first (canonical store with full accumulated history)
        df = _load_from_db(ticker, days=days)
        if not df.empty:
            return df          # already filtered by SQL

        # Try parquet (cumulative cache — works offline)
        df = _load_from_parquet(ticker)
        if not df.empty:
            return _apply_days_filter(df, days)

        # Fallback to yfinance
        df = _load_from_yfinance(ticker, days=days)
        return df              # already filtered by period/start-end

    if source == "parquet":
        return _apply_days_filter(_load_from_parquet(ticker), days)
    elif source == "db":
        return _load_from_db(ticker, days=days)
    elif source == "yfinance":
        return _load_from_yfinance(ticker, days=days)
    else:
        logger.warning(f"Unknown source '{source}' for {ticker}")
        return pd.DataFrame()


def load_universe_ohlcv(
    tickers: list[str],
    source: str = "auto",
    days: int | None = None,
) -> dict[str, pd.DataFrame]:
    """
    Load OHLCV for multiple tickers.

    Parameters
    ----------
    tickers : list[str]
        Symbols to load.
    source : str
        Data source — see :func:`load_ohlcv`.
    days : int, optional
        Limit each ticker's data to the most recent *days*
        calendar days.

    Returns
    -------
    dict[str, pd.DataFrame]
        ``{ticker: DataFrame}`` for successfully loaded symbols.
        Failed tickers are logged and skipped.
    """
    # Pre-warm the parquet cache if using auto/parquet
    if source in ("auto", "parquet"):
        _ensure_parquet_cached()

    result: dict[str, pd.DataFrame] = {}
    missing: list[str] = []

    for ticker in tickers:
        try:
            df = load_ohlcv(ticker, source=source, days=days)
            if not df.empty:
                result[ticker] = df
            else:
                missing.append(ticker)
        except Exception as e:
            logger.warning(f"Failed to load {ticker}: {e}")
            missing.append(ticker)

    logger.info(
        f"Loaded {len(result)}/{len(tickers)} tickers"
        + (f" (missing: {len(missing)})" if missing else "")
        + (f" [days={days}]" if days else "")
    )

    if missing and len(missing) <= 20:
        logger.debug(f"Missing tickers: {missing}")

    return result


def get_available_tickers(source: str = "parquet") -> list[str]:
    """
    Return list of tickers available in the data source.
    """
    if source == "parquet":
        _ensure_parquet_cached()
        tickers = set()
        for path, df in _parquet_cache.items():
            if "_sym_col" in df.attrs:
                sym_col = df.attrs["_sym_col"]
                tickers.update(df[sym_col].unique().tolist())
        return sorted(tickers)

    return []


def data_summary() -> dict:
    """
    Quick summary of available data files and coverage.
    """
    info = {}

    for label, path in [
        ("universe", _UNIVERSE_PARQUET),
        ("india", _INDIA_PARQUET),
    ]:
        if path.exists():
            df = _read_parquet_raw(path)
            sym_col = _find_symbol_col(df)
            date_col = _find_date_col(df)

            entry = {
                "path": str(path),
                "size_mb": round(path.stat().st_size / (1024 * 1024), 1),
                "rows": len(df),
            }
            if sym_col:
                entry["tickers"] = int(df[sym_col].nunique())
                entry["ticker_list"] = sorted(df[sym_col].unique().tolist())
            if date_col:
                dates = pd.to_datetime(df[date_col])
                entry["date_min"] = str(dates.min().date())
                entry["date_max"] = str(dates.max().date())

            info[label] = entry
        else:
            info[label] = {"path": str(path), "exists": False}

    return info


# ═══════════════════════════════════════════════════════════════
#  PARQUET LOADING
# ═══════════════════════════════════════════════════════════════

def _ensure_parquet_cached() -> None:
    """Load parquet files into module-level cache if not already."""
    for path in [_UNIVERSE_PARQUET, _INDIA_PARQUET]:
        if path.exists() and path not in _parquet_cache:
            try:
                df = _read_parquet_raw(path)
                sym_col = _find_symbol_col(df)
                if sym_col:
                    df.attrs["_sym_col"] = sym_col
                    _parquet_cache[path] = df
                    n_syms = df[sym_col].nunique()
                    logger.info(
                        f"Cached {path.name}: {len(df):,} rows, "
                        f"{n_syms} symbols"
                    )
                else:
                    logger.warning(
                        f"No symbol column in {path.name}, "
                        f"columns: {list(df.columns)}"
                    )
            except Exception as e:
                logger.warning(f"Failed to cache {path.name}: {e}")


def _load_from_parquet(ticker: str) -> pd.DataFrame:
    """Extract a single ticker's OHLCV from cached parquet data."""
    _ensure_parquet_cached()

    for path, df in _parquet_cache.items():
        sym_col = df.attrs.get("_sym_col")
        if sym_col is None:
            continue

        mask = df[sym_col] == ticker
        if not mask.any():
            # Try case-insensitive
            mask = df[sym_col].str.upper() == ticker.upper()

        if mask.any():
            subset = df[mask].copy()
            return _normalise(subset)

    return pd.DataFrame()


def _read_parquet_raw(path: Path) -> pd.DataFrame:
    """Read a parquet file, resetting any index."""
    df = pd.read_parquet(path)
    if isinstance(df.index, pd.DatetimeIndex) or df.index.name in (
        "Date", "date", "trade_date",
    ):
        df = df.reset_index()
    return df


# ═══════════════════════════════════════════════════════════════
#  DATABASE LOADING
# ═══════════════════════════════════════════════════════════════

def _load_from_db(
    ticker: str,
    days: int | None = None,
) -> pd.DataFrame:
    """
    Load from PostgreSQL regional cash tables via SQLAlchemy.

    Uses a module-level SQLAlchemy engine so pandas receives a
    proper connectable — no more DBAPI2 UserWarning.

    When *days* is given the SQL WHERE clause restricts to
    ``date >= today - days`` so only the needed rows are
    transferred from the database.
    """
    try:
        from sqlalchemy import text
    except ImportError:
        logger.debug("sqlalchemy not installed — skipping DB source")
        return pd.DataFrame()

    table = _ticker_to_cash_table(ticker)

    try:
        engine = _get_engine()

        with engine.connect() as conn:
            if days is not None:
                cutoff = (
                    datetime.now() - timedelta(days=days)
                ).strftime("%Y-%m-%d")
                query = text(f"""
                    SELECT date, open, high, low, close, volume
                    FROM {table}
                    WHERE symbol = :symbol AND date >= :cutoff
                    ORDER BY date ASC
                """)
                df = pd.read_sql(query, conn, params={
                    "symbol": ticker,
                    "cutoff": cutoff,
                })
            else:
                query = text(f"""
                    SELECT date, open, high, low, close, volume
                    FROM {table}
                    WHERE symbol = :symbol
                    ORDER BY date ASC
                """)
                df = pd.read_sql(query, conn, params={
                    "symbol": ticker,
                })

        if df.empty:
            return pd.DataFrame()

        return _normalise(df)

    except Exception as e:
        logger.debug(f"DB load failed for {ticker} from {table}: {e}")
        return pd.DataFrame()


def _ticker_to_cash_table(ticker: str) -> str:
    """Map ticker to the correct regional cash table name."""
    t = ticker.upper()
    if t.endswith(".HK"):
        return "hk_cash"
    elif t.endswith(".NS") or t.endswith(".BO"):
        return "in_cash"
    elif "." not in t:
        return "us_cash"
    else:
        return "others_cash"


# ═══════════════════════════════════════════════════════════════
#  YFINANCE LOADING
# ═══════════════════════════════════════════════════════════════

def _load_from_yfinance(
    ticker: str,
    period: str = "2y",
    days: int | None = None,
) -> pd.DataFrame:
    """
    Load from yfinance as last-resort fallback.

    When *days* is given, a start/end date range is used instead
    of the less-precise ``period`` string, ensuring we fetch
    exactly the window we need.
    """
    try:
        import yfinance as yf
    except ImportError:
        logger.warning("yfinance not installed — cannot fallback")
        return pd.DataFrame()

    try:
        if days is not None:
            end_dt = datetime.now()
            start_dt = end_dt - timedelta(days=days)
            raw = yf.download(
                ticker,
                start=start_dt.strftime("%Y-%m-%d"),
                end=end_dt.strftime("%Y-%m-%d"),
                progress=False,
                auto_adjust=False,
            )
        else:
            raw = yf.download(
                ticker,
                period=period,
                progress=False,
                auto_adjust=False,
            )

        if raw.empty:
            return pd.DataFrame()
        return _normalise(raw)

    except Exception as e:
        logger.debug(f"yfinance failed for {ticker}: {e}")
        return pd.DataFrame()


# ═══════════════════════════════════════════════════════════════
#  NORMALISATION
# ═══════════════════════════════════════════════════════════════

def _normalise(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise any OHLCV DataFrame to the standard format
    expected by compute/:

      - Columns: open, high, low, close, volume (lowercase)
      - DatetimeIndex named "date", sorted ascending
      - No duplicate dates
      - No rows where close is NaN or zero
    """
    df = df.copy()

    # ── Flatten MultiIndex columns (yfinance multi-ticker) ────
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            c[0] if isinstance(c, tuple) else c
            for c in df.columns
        ]

    # ── Lowercase column names ────────────────────────────────
    df.columns = [str(c).lower().strip() for c in df.columns]

    # ── Rename known variants to canonical names ──────────────
    _COLUMN_RENAMES = {
        "adj close":  "adj_close",
        "trade_date": "date",
    }
    df.rename(
        columns={k: v for k, v in _COLUMN_RENAMES.items() if k in df.columns},
        inplace=True,
    )

    # ── Set date index ────────────────────────────────────────
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)
    elif not isinstance(df.index, pd.DatetimeIndex):
        try:
            df.index = pd.to_datetime(df.index)
        except Exception:
            logger.debug("Cannot convert index to datetime")
            return pd.DataFrame()

    df.index.name = "date"

    # ── Drop non-OHLCV columns ────────────────────────────────
    keep = [c for c in df.columns if c in _REQUIRED_COLS]
    missing = [c for c in _REQUIRED_COLS if c not in df.columns]

    if missing:
        logger.debug(
            f"Missing required columns after normalisation: {missing}. "
            f"Available: {list(df.columns)}"
        )
        return pd.DataFrame()

    df = df[keep]

    # ── Clean ─────────────────────────────────────────────────
    df = df.sort_index()
    df = df[~df.index.duplicated(keep="last")]
    df = df[df["close"].notna() & (df["close"] > 0)]

    for col in _REQUIRED_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["open", "high", "low", "close"])
    df["volume"] = df["volume"].fillna(0).astype(np.int64)

    return df


# ═══════════════════════════════════════════════════════════════
#  COLUMN FINDERS
# ═══════════════════════════════════════════════════════════════

def _find_symbol_col(df: pd.DataFrame) -> str | None:
    """Find the symbol/ticker column in a DataFrame."""
    for candidate in ["symbol", "Symbol", "ticker", "Ticker",
                      "SYMBOL", "TICKER"]:
        if candidate in df.columns:
            return candidate
    if df.index.name in ("symbol", "ticker"):
        name = df.index.name
        df.reset_index(inplace=True)
        return name
    return None


def _find_date_col(df: pd.DataFrame) -> str | None:
    """Find the date column in a DataFrame."""
    for candidate in ["Date", "date", "trade_date", "Trade_Date",
                      "DATE", "timestamp"]:
        if candidate in df.columns:
            return candidate
    if isinstance(df.index, pd.DatetimeIndex):
        return df.index.name or "index"
    return None


# ═══════════════════════════════════════════════════════════════
#  CLI — Quick test / diagnostics
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse
    import json

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
    )

    parser = argparse.ArgumentParser(description="Data loader diagnostics")
    parser.add_argument(
        "--days", type=int, default=None,
        help="Only show the most recent N calendar days of data",
    )
    parser.add_argument(
        "--tickers", nargs="+", default=["SPY", "QQQ", "XLK"],
        help="Tickers to test (default: SPY QQQ XLK)",
    )
    parser.add_argument(
        "--source", default="auto",
        choices=["auto", "db", "parquet", "yfinance"],
        help="Force data source (default: auto = db → parquet → yfinance)",
    )
    cli_args = parser.parse_args()

    print("\n" + "=" * 60)
    print("  DATA LOADER — Diagnostics")
    if cli_args.days:
        print(f"  Days filter: {cli_args.days}")
    print(f"  Source priority: {cli_args.source}")
    print("=" * 60)

    # Summary
    summary = data_summary()
    for label, info in summary.items():
        print(f"\n  {label.upper()}:")
        if info.get("exists") is False:
            print(f"    File: {info['path']}  — NOT FOUND")
        else:
            print(f"    File:    {info['path']}")
            print(f"    Size:    {info.get('size_mb', '?')} MB")
            print(f"    Rows:    {info.get('rows', '?'):,}")
            print(f"    Tickers: {info.get('tickers', '?')}")
            print(f"    Range:   {info.get('date_min', '?')} → "
                  f"{info.get('date_max', '?')}")

    # Test loading
    print(f"\n  Test loading: {cli_args.tickers}"
          + (f" (last {cli_args.days} days)" if cli_args.days else ""))
    for t in cli_args.tickers:
        df = load_ohlcv(t, source=cli_args.source, days=cli_args.days)
        if df.empty:
            print(f"    {t}: NO DATA")
        else:
            print(f"    {t}: {len(df)} bars, "
                  f"{df.index[0].date()} → {df.index[-1].date()}, "
                  f"close={df['close'].iloc[-1]:.2f}")

    print("\n" + "=" * 60)
####################################
"""
src/db/load_db.py — Load parquet/CSV data into PostgreSQL (upsert).

Uses INSERT ... ON CONFLICT DO UPDATE so the script is safe to
re-run at any time.  Duplicate rows (by the table's unique key)
are updated in place rather than rejected.

Usage:
    python src/db/load_db.py                            # all markets, all types
    python src/db/load_db.py --market us                 # US only
    python src/db/load_db.py --type cash                 # cash tables only
    python src/db/load_db.py --type options              # options tables only
    python src/db/load_db.py --market us --type cash     # US cash only
    python src/db/load_db.py --dry-run                   # preview, no DB writes
    python src/db/load_db.py --status                    # show row counts only
"""

import sys
from pathlib import Path

_SRC  = Path(__file__).resolve().parent.parent
_ROOT = _SRC.parent
for _p in (str(_ROOT), str(_SRC)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import argparse
import logging
import math

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from common.credentials import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)-20s %(levelname)-10s %(message)s",
)
LOG = logging.getLogger(__name__)

DATA_DIR = _ROOT / "data"

CASH_REGIONS    = ["us", "hk", "in"]
OPTIONS_REGIONS = ["us", "hk"]

BATCH_SIZE = 2000


# ═══════════════════════════════════════════════════════════════
#  CONNECTION
# ═══════════════════════════════════════════════════════════════

def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
    )


# ═══════════════════════════════════════════════════════════════
#  STATUS — row counts for every table in the database
# ═══════════════════════════════════════════════════════════════

# Tables we expect to exist (in display order).
# Any table found in the DB but not listed here is still shown,
# appended at the end.
KNOWN_TABLES = [
    "us_cash",
    "hk_cash",
    "india_cash",
    "us_options",
    "hk_options",
]


def _discover_tables(conn) -> list[str]:
    """
    Return all user tables in the public schema, ordered so that
    KNOWN_TABLES appear first (in their defined order) followed
    by any extras alphabetically.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT table_name
        FROM   information_schema.tables
        WHERE  table_schema = 'public'
          AND  table_type   = 'BASE TABLE'
        ORDER  BY table_name
        """
    )
    all_tables = [row[0] for row in cur.fetchall()]
    cur.close()

    # Preserve KNOWN_TABLES ordering, then append anything else
    ordered: list[str] = []
    seen: set[str] = set()
    for t in KNOWN_TABLES:
        if t in all_tables:
            ordered.append(t)
            seen.add(t)
    for t in all_tables:
        if t not in seen:
            ordered.append(t)

    return ordered


def load_status() -> None:
    """Show row counts for all tables in the database."""
    conn = get_conn()
    cur = conn.cursor()

    try:
        tables = _discover_tables(conn)

        LOG.info("=" * 55)
        LOG.info(f"  {'Table':<30s} {'Rows':>12s}  {'Date range':>25s}")
        LOG.info("-" * 55)

        for table in tables:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {table}")  # noqa: S608
                count = cur.fetchone()[0]

                # Best-effort date range (works for tables with a 'date' column)
                date_range = ""
                try:
                    cur.execute(
                        f"SELECT MIN(date)::text, MAX(date)::text FROM {table}"  # noqa: S608
                    )
                    row = cur.fetchone()
                    if row and row[0] and row[1]:
                        date_range = f"{row[0]} → {row[1]}"
                except Exception:
                    pass  # table has no 'date' column — that's fine

                LOG.info(f"  {table:<30s} {count:>12,}  {date_range:>25s}")

            except Exception:
                LOG.info(f"  {table:<30s} {'(error)':>12s}")

        LOG.info("=" * 55)

    finally:
        cur.close()
        conn.close()


# ═══════════════════════════════════════════════════════════════
#  TYPE SANITISATION  (numpy/pandas → Python natives)
# ═══════════════════════════════════════════════════════════════

def _sanitize(val):
    """Convert numpy/pandas types to Python natives for psycopg2."""
    if val is None:
        return None
    if isinstance(val, (np.integer,)):
        return int(val)
    if isinstance(val, (np.floating,)):
        v = float(val)
        return None if (math.isnan(v) or math.isinf(v)) else v
    if isinstance(val, (np.bool_,)):
        return bool(val)
    if isinstance(val, (pd.Timestamp,)):
        return None if pd.isna(val) else val.to_pydatetime().date()
    if isinstance(val, np.datetime64):
        if pd.isna(val):
            return None
        return pd.Timestamp(val).to_pydatetime().date()
    # Catch-all for other pandas NA types (pd.NA, pd.NaT, etc.)
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    return val


def _to_tuples(df: pd.DataFrame, columns: list[str]) -> list[tuple]:
    """Convert DataFrame rows to a list of sanitised tuples."""
    return [
        tuple(_sanitize(v) for v in row)
        for row in df[columns].itertuples(index=False, name=None)
    ]


# ═══════════════════════════════════════════════════════════════
#  BATCH UPSERT ENGINE
# ═══════════════════════════════════════════════════════════════

def _batch_upsert(
    table: str,
    sql_template: str,
    rows: list[tuple],
) -> int:
    """
    Execute batched INSERT ... ON CONFLICT DO UPDATE.
    Returns the number of rows processed.
    """
    if not rows:
        return 0

    sql = sql_template.format(table=table)
    conn = get_conn()
    cur = conn.cursor()
    total = 0

    try:
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i : i + BATCH_SIZE]
            execute_values(cur, sql, batch, page_size=BATCH_SIZE)
            total += len(batch)

            if total % 10_000 == 0 or total == len(rows):
                LOG.info(f"  {table}: {total:,} / {len(rows):,} rows upserted")

        conn.commit()
        LOG.info(f"  {table}: DONE — {total:,} rows upserted")

    except Exception as e:
        conn.rollback()
        LOG.error(f"  {table}: upsert failed at row ~{total:,}: {e}")
        raise
    finally:
        cur.close()
        conn.close()

    return total


# ═══════════════════════════════════════════════════════════════
#  CASH LOADING
# ═══════════════════════════════════════════════════════════════

CASH_COLS = ["date", "symbol", "open", "high", "low", "close", "volume"]

CASH_UPSERT_SQL = """
INSERT INTO {table} (date, symbol, open, high, low, close, volume)
VALUES %s
ON CONFLICT (date, symbol) DO UPDATE SET
    open   = EXCLUDED.open,
    high   = EXCLUDED.high,
    low    = EXCLUDED.low,
    close  = EXCLUDED.close,
    volume = EXCLUDED.volume
"""


def load_cash(market: str, dry_run: bool = False) -> int:
    """Read {market}_cash.parquet → upsert into {market}_cash table."""
    path = DATA_DIR / f"{market}_cash.parquet"
    if not path.exists():
        LOG.warning(f"Not found: {path}")
        return 0

    df = pd.read_parquet(path)
    df.columns = [str(c).lower().strip() for c in df.columns]

    if "date" not in df.columns:
        LOG.error(f"No 'date' column in {path.name}  (cols: {list(df.columns)})")
        return 0

    df["date"] = pd.to_datetime(df["date"]).dt.date

    missing = [c for c in CASH_COLS if c not in df.columns]
    if missing:
        LOG.error(f"Missing columns in {path.name}: {missing}")
        return 0

    # ── Deduplicate ────────────────────────────────────────────
    before = len(df)
    df = df.drop_duplicates(subset=["date", "symbol"], keep="last")
    dupes = before - len(df)
    if dupes:
        LOG.info(f"  {path.name}: removed {dupes:,} duplicate rows before load")

    table = f"{market}_cash"
    LOG.info(
        f"Loading {len(df):,} rows → {table}  "
        f"({df['symbol'].nunique()} symbols, "
        f"{df['date'].min()} → {df['date'].max()})"
    )

    if dry_run:
        LOG.info("  [DRY RUN] — skipped DB write")
        return len(df)

    rows = _to_tuples(df, CASH_COLS)
    return _batch_upsert(table, CASH_UPSERT_SQL, rows)


# ═══════════════════════════════════════════════════════════════
#  OPTIONS LOADING
# ═══════════════════════════════════════════════════════════════

OPTIONS_DEDUP_KEYS = ["date", "symbol", "expiry", "strike", "opt_type"]

OPTIONS_COLS = [
    "date", "symbol", "expiry", "strike", "opt_type",
    "bid", "ask", "last", "volume", "oi", "iv",
    "delta", "gamma", "theta", "vega", "rho",
    "underlying_price", "dte", "source",
]

OPTIONS_UPSERT_SQL = """
INSERT INTO {table} (
    date, symbol, expiry, strike, opt_type,
    bid, ask, last, volume, oi, iv,
    delta, gamma, theta, vega, rho,
    underlying_price, dte, source
)
VALUES %s
ON CONFLICT (date, symbol, expiry, strike, opt_type) DO UPDATE SET
    bid              = EXCLUDED.bid,
    ask              = EXCLUDED.ask,
    last             = EXCLUDED.last,
    volume           = EXCLUDED.volume,
    oi               = EXCLUDED.oi,
    iv               = EXCLUDED.iv,
    delta            = EXCLUDED.delta,
    gamma            = EXCLUDED.gamma,
    theta            = EXCLUDED.theta,
    vega             = EXCLUDED.vega,
    rho              = EXCLUDED.rho,
    underlying_price = EXCLUDED.underlying_price,
    dte              = EXCLUDED.dte,
    source           = EXCLUDED.source
"""


def load_options(market: str, dry_run: bool = False) -> int:
    """Read per-ticker CSVs from data/options/{market}/ → upsert into {market}_options."""
    csv_dir = DATA_DIR / "options" / market
    if not csv_dir.exists():
        LOG.warning(f"Not found: {csv_dir}")
        return 0

    frames = []
    for csv_file in sorted(csv_dir.glob("*.csv")):
        try:
            tmp = pd.read_csv(csv_file, dtype={"date": str, "expiry": str})
            if not tmp.empty:
                frames.append(tmp)
        except Exception as e:
            LOG.warning(f"  Skipping {csv_file.name}: {e}")

    if not frames:
        LOG.warning(f"No CSV files in {csv_dir}")
        return 0

    df = pd.concat(frames, ignore_index=True)
    df.columns = [str(c).lower().strip() for c in df.columns]

    # ── Parse dates ────────────────────────────────────────────
    df["date"]   = pd.to_datetime(df["date"]).dt.date
    df["expiry"] = pd.to_datetime(df["expiry"]).dt.date

    # ── Compute DTE if missing ─────────────────────────────────
    if "dte" not in df.columns:
        df["dte"] = df.apply(
            lambda r: (r["expiry"] - r["date"]).days
            if pd.notna(r["expiry"]) and pd.notna(r["date"])
            else None,
            axis=1,
        )

    if "source" not in df.columns:
        df["source"] = "yfinance"

    # ── Fill any missing optional columns with None ────────────
    for col in OPTIONS_COLS:
        if col not in df.columns:
            df[col] = None

    # ── Deduplicate ────────────────────────────────────────────
    before = len(df)
    df = df.drop_duplicates(subset=OPTIONS_DEDUP_KEYS, keep="last")
    dupes = before - len(df)
    if dupes:
        LOG.info(f"  Removed {dupes:,} duplicate rows before load")

    table = f"{market}_options"
    LOG.info(
        f"Loading {len(df):,} rows → {table}  "
        f"({df['symbol'].nunique()} symbols)"
    )

    if dry_run:
        LOG.info("  [DRY RUN] — skipped DB write")
        return len(df)

    rows = _to_tuples(df, OPTIONS_COLS)
    return _batch_upsert(table, OPTIONS_UPSERT_SQL, rows)


# ═══════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Load parquet/CSV → PostgreSQL (upsert, idempotent)",
    )
    parser.add_argument(
        "--market",
        choices=["us", "hk", "in", "all"],
        default="all",
    )
    parser.add_argument(
        "--type",
        choices=["cash", "options", "all"],
        default="all",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview row counts — no DB writes",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show row counts for all tables and exit (no loading)",
    )
    args = parser.parse_args()

    # ── Status-only mode: print table counts and exit ──────────
    if args.status:
        load_status()
        return

    if args.market == "all":
        cash_markets    = CASH_REGIONS
        options_markets = OPTIONS_REGIONS
    else:
        cash_markets    = [args.market] if args.market in CASH_REGIONS else []
        options_markets = [args.market] if args.market in OPTIONS_REGIONS else []

    LOG.info("=" * 60)
    LOG.info("LOAD DB — upsert from parquet / CSV")
    if args.dry_run:
        LOG.info("  *** DRY RUN — no DB writes ***")
    LOG.info("=" * 60)

    total = 0

    if args.type in ("cash", "all"):
        for mkt in cash_markets:
            total += load_cash(mkt, dry_run=args.dry_run)

    if args.type in ("options", "all"):
        for mkt in options_markets:
            total += load_options(mkt, dry_run=args.dry_run)

    LOG.info("=" * 60)
    LOG.info(f"TOTAL: {total:,} rows processed")
    LOG.info("=" * 60)

    # ── Show status after loading ──────────────────────────────
    load_status()


if __name__ == "__main__":
    main()

####################################################
"""
src/db/db.py

Database connection utilities.
All table definitions live in schema.py — this file only provides
the connection engine and health check.
"""
import sys
from pathlib import Path

_SRC  = Path(__file__).resolve().parent.parent  # .../src
_ROOT = _SRC.parent                             # .../SmartMoneyRotation
for _p in (str(_ROOT), str(_SRC)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import logging
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

SRC = Path(__file__).resolve().parent.parent
ROOT = SRC.parent
sys.path.insert(0, str(ROOT))

from common.credentials import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS

LOG = logging.getLogger(__name__)

# ── Connection string ─────────────────────────────────────────
DATABASE_URL = (
    f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# Singleton engine (reused across the process)
_engine: Engine | None = None


def get_engine() -> Engine:
    """
    Return a SQLAlchemy engine (singleton per process).

    Uses connection pooling — safe to call repeatedly.
    """
    global _engine
    if _engine is None:
        _engine = create_engine(
            DATABASE_URL,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=1800,
            echo=False,
        )
    return _engine


def test_connection() -> bool:
    """Verify DB is reachable and responsive."""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).scalar()
            if result == 1:
                LOG.info(
                    f"DB connection OK → "
                    f"{DB_HOST}:{DB_PORT}/{DB_NAME}"
                )
                return True
    except Exception as e:
        LOG.error(f"DB connection FAILED: {e}")
    return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_connection()

########################################