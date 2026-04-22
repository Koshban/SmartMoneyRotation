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

    elif market == "india":
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
    elif market == "india":
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
    """
    Bulk download OHLCV via yfinance.

    If *start_date* and *end_date* are given (YYYY-MM-DD strings)
    they take precedence over *period*.  This is the path used
    when the caller specifies ``--days``.
    """
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

    # ── Reshape multi-ticker result ────────────────────────────
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
        # Compute start/end if days is set for yfinance fallback
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
    """
    Download OHLCV for all symbols across requested markets.
    Auto-selects yfinance vs IBKR based on period length.

    If *days* is given it takes precedence over *period*: a
    start/end date range is computed and passed to yfinance, or
    converted to an IBKR duration string.

    Saves:
      data/{market}_cash.parquet   — per-market (for load_db.py)
      data/universe_ohlcv.parquet  — combined   (for loader.py)
    """
    DATA_DIR.mkdir(exist_ok=True)
    all_dfs = []

    # Pre-compute date range when --days is in play
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

        # ── Save per-market parquet (what load_db.py expects) ──
        market_path = DATA_DIR / f"{market}_cash.parquet"
        df.to_parquet(market_path, index=False)
        size_mb = market_path.stat().st_size / (1024 * 1024)
        logger.info(
            f"Saved → {market_path}  "
            f"({size_mb:.1f} MB, {len(df):,} rows, "
            f"{df['symbol'].nunique()} symbols)"
        )

        all_dfs.append(df)

    if not all_dfs:
        logger.warning("No data collected across any market")
        return

    # ── Save combined file (for loader.py parquet reads) ───────
    combined = pd.concat(all_dfs, ignore_index=True)
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
        choices=["us", "hk", "india", "all"],
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
        markets = ["us", "hk", "india"]
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