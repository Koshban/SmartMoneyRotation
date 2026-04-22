"""
src/db/load_db.py

Load parquet / CSV files into PostgreSQL tables defined in schema.py.

Usage:
    python src/db/load_db.py --market all --type all
    python src/db/load_db.py --market us  --type cash
    python src/db/load_db.py --market us  --type options
    python src/db/load_db.py --market hk  --type options
    python src/db/load_db.py --status
"""
import sys
from pathlib import Path

_SRC  = Path(__file__).resolve().parent.parent  # .../src
_ROOT = _SRC.parent                             # .../SmartMoneyRotation
for _p in (str(_ROOT), str(_SRC)):
    if _p not in sys.path:
        sys.path.insert(0, _p)
import argparse
import logging
import pandas as pd
from sqlalchemy import text
import numpy as np

SRC = Path(__file__).resolve().parent.parent
ROOT = SRC.parent
sys.path.insert(0, str(ROOT))

from db.db import get_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)-20s %(levelname)-10s %(message)s",
)
LOG = logging.getLogger(__name__)

DATA_DIR = ROOT / "data"


# ═══════════════════════════════════════════════════════════════
#  COLUMN MAPS  — parquet/CSV column → DB column
# ═══════════════════════════════════════════════════════════════

CASH_COLUMNS = [
    "date", "symbol", "open", "high", "low", "close", "volume",
]

OPTIONS_COLUMNS = [
    "date", "symbol", "expiry", "strike", "opt_type",
    "bid", "ask", "last", "volume", "oi",
    "iv",
    "delta", "gamma", "theta", "vega", "rho",
    "underlying_price", "dte",
    "source",
]


# ═══════════════════════════════════════════════════════════════
#  CASH LOADING
# ═══════════════════════════════════════════════════════════════

def load_cash(market: str) -> int:
    """
    Load cash OHLCV data into {market}_cash table.

    Reads from: data/{market}_cash.parquet  (or .csv fallback)
    Upsert:     ON CONFLICT (date, symbol) DO UPDATE
    """
    table = f"{market}_cash"
    df = _read_data_file(market, "cash")

    if df is None or df.empty:
        LOG.warning(f"No data found for {table}")
        return 0

    # Ensure required columns exist
    for col in ["date", "symbol", "close"]:
        if col not in df.columns:
            LOG.error(f"{table}: missing required column '{col}'")
            return 0

    # Keep only known columns, fill missing optional ones with None
    for col in CASH_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[CASH_COLUMNS].copy()

    # Clean
    df = df.dropna(subset=["date", "symbol", "close"])
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df.drop_duplicates(subset=["date", "symbol"], keep="last")

    if df.empty:
        LOG.warning(f"{table}: no valid rows after cleaning")
        return 0

    # Upsert
    engine = get_engine()
    upsert_sql = f"""
        INSERT INTO {table} (date, symbol, open, high, low, close, volume)
        VALUES (:date, :symbol, :open, :high, :low, :close, :volume)
        ON CONFLICT (date, symbol) DO UPDATE SET
            open   = EXCLUDED.open,
            high   = EXCLUDED.high,
            low    = EXCLUDED.low,
            close  = EXCLUDED.close,
            volume = EXCLUDED.volume;
    """

    rows_loaded = _batch_upsert(engine, upsert_sql, df, table)
    return rows_loaded


# ═══════════════════════════════════════════════════════════════
#  OPTIONS LOADING
# ═══════════════════════════════════════════════════════════════

def load_options(market: str) -> int:
    """
    Load options snapshot data into {market}_options table.

    Reads from: data/{market}_options.parquet  (or .csv fallback)
    Upsert:     ON CONFLICT (date, symbol, expiry, strike, opt_type) DO UPDATE

    Handles both yfinance data (greeks are NULL) and IBKR data (full greeks).
    When IBKR data overwrites yfinance data for the same contract, greeks
    get populated.
    """
    table = f"{market}_options"
    df = _read_data_file(market, "options")

    if df is None or df.empty:
        LOG.warning(f"No data found for {table}")
        return 0

    # Ensure required columns exist
    for col in ["date", "symbol", "expiry", "strike", "opt_type"]:
        if col not in df.columns:
            LOG.error(f"{table}: missing required column '{col}'")
            return 0

    # Add missing optional columns as None
    for col in OPTIONS_COLUMNS:
        if col not in df.columns:
            df[col] = None

    # Compute DTE if not present
    if df["dte"].isna().all():
        try:
            df["dte"] = (
                pd.to_datetime(df["expiry"]) - pd.to_datetime(df["date"])
            ).dt.days
        except Exception:
            pass

    df = df[OPTIONS_COLUMNS].copy()

    # Clean
    df = df.dropna(subset=["date", "symbol", "expiry", "strike", "opt_type"])
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["expiry"] = pd.to_datetime(df["expiry"]).dt.date
    df["opt_type"] = df["opt_type"].str.upper().str.strip()
    df = df[df["opt_type"].isin(["C", "P"])]
    df = df.drop_duplicates(
        subset=["date", "symbol", "expiry", "strike", "opt_type"],
        keep="last",
    )

    if df.empty:
        LOG.warning(f"{table}: no valid rows after cleaning")
        return 0
    # ── Fix types: parquet stores volume/oi as DOUBLE (can contain inf/NaN)
    #    but PostgreSQL expects INTEGER ──
    
    df = df.replace([np.inf, -np.inf], np.nan)
    df["volume"] = df["volume"].astype("Int64")
    df["oi"]     = df["oi"].astype("Int64")
    df["dte"]    = df["dte"].astype("Int64")

    # Upsert — IBKR data (with greeks) overwrites yfinance data (without)
    # Upsert — IBKR data (with greeks) overwrites yfinance data (without)
    engine = get_engine()
    upsert_sql = f"""
        INSERT INTO {table} (
            date, symbol, expiry, strike, opt_type,
            bid, ask, last, volume, oi,
            iv,
            delta, gamma, theta, vega, rho,
            underlying_price, dte,
            source
        )
        VALUES (
            :date, :symbol, :expiry, :strike, :opt_type,
            :bid, :ask, :last, :volume, :oi,
            :iv,
            :delta, :gamma, :theta, :vega, :rho,
            :underlying_price, :dte,
            :source
        )
        ON CONFLICT (date, symbol, expiry, strike, opt_type) DO UPDATE SET
            bid              = COALESCE(EXCLUDED.bid,              {table}.bid),
            ask              = COALESCE(EXCLUDED.ask,              {table}.ask),
            last             = COALESCE(EXCLUDED.last,             {table}.last),
            volume           = COALESCE(EXCLUDED.volume,           {table}.volume),
            oi               = COALESCE(EXCLUDED.oi,               {table}.oi),
            iv               = COALESCE(EXCLUDED.iv,               {table}.iv),
            delta            = COALESCE(EXCLUDED.delta,            {table}.delta),
            gamma            = COALESCE(EXCLUDED.gamma,            {table}.gamma),
            theta            = COALESCE(EXCLUDED.theta,            {table}.theta),
            vega             = COALESCE(EXCLUDED.vega,             {table}.vega),
            rho              = COALESCE(EXCLUDED.rho,              {table}.rho),
            underlying_price = COALESCE(EXCLUDED.underlying_price, {table}.underlying_price),
            dte              = COALESCE(EXCLUDED.dte,              {table}.dte),
            source           = COALESCE(EXCLUDED.source,           {table}.source);
    """

    rows_loaded = _batch_upsert(engine, upsert_sql, df, table)
    return rows_loaded


# ═══════════════════════════════════════════════════════════════
#  SHARED HELPERS
# ═══════════════════════════════════════════════════════════════

def _read_data_file(market: str, dtype: str) -> pd.DataFrame | None:
    """
    Read data from parquet (preferred) or CSV fallback.

    Looks for:  data/{market}_{dtype}.parquet
                data/{market}_{dtype}.csv
    """
    parquet_path = DATA_DIR / f"{market}_{dtype}.parquet"
    csv_path = DATA_DIR / f"{market}_{dtype}.csv"

    if parquet_path.exists():
        LOG.info(f"Reading {parquet_path.name}")
        return pd.read_parquet(parquet_path)
    elif csv_path.exists():
        LOG.info(f"Reading {csv_path.name} (parquet not found)")
        return pd.read_csv(csv_path)
    else:
        LOG.warning(
            f"No data file found: {parquet_path.name} or {csv_path.name}"
        )
        return None


def _batch_upsert(
    engine,
    sql: str,
    df: pd.DataFrame,
    table: str,
    batch_size: int = 1000,
) -> int:
    """
    Execute upsert in batches for memory efficiency.

    Converts NaN/NaT to None for proper NULL handling in SQL.
    """
    # Replace NaN with None (psycopg2 sends NULL)
    records = df.where(df.notna(), None).to_dict("records")

    total = 0
    with engine.begin() as conn:
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            conn.execute(text(sql), batch)
            total += len(batch)

            if total % 5000 == 0 or total == len(records):
                LOG.info(f"  {table}: {total:,} / {len(records):,} rows")

    LOG.info(f"  {table}: loaded {total:,} rows total")
    return total


def load_status():
    """Show row counts for all tables."""
    engine = get_engine()
    from db.schema import all_table_names

    LOG.info("=" * 50)
    LOG.info(f"{'Table':<20s} {'Rows':>10s}")
    LOG.info("-" * 50)

    with engine.connect() as conn:
        for table in all_table_names():
            try:
                result = conn.execute(
                    text(f"SELECT COUNT(*) FROM {table}")
                ).scalar()
                LOG.info(f"{table:<20s} {result:>10,}")
            except Exception:
                LOG.info(f"{table:<20s} {'(missing)':>10s}")

    LOG.info("=" * 50)


# ═══════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════

MARKET_CHOICES = ["us", "hk", "india", "others", "all"]
TYPE_CHOICES = ["cash", "options", "all"]

def main():
    parser = argparse.ArgumentParser(
        description="Load parquet/CSV data into PostgreSQL",
    )
    parser.add_argument(
        "--market",
        choices=MARKET_CHOICES,
        default="all",
    )
    parser.add_argument(
        "--type",
        choices=TYPE_CHOICES,
        default="all",
        dest="dtype",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show table row counts",
    )
    args = parser.parse_args()

    if args.status:
        load_status()
        return

    # ── Determine what to load ────────────────────────────────
    from db.schema import CASH_REGIONS, OPTIONS_REGIONS

    if args.market == "all":
        cash_markets = CASH_REGIONS
        opt_markets = OPTIONS_REGIONS
    else:
        cash_markets = [args.market] if args.market in CASH_REGIONS else []
        opt_markets = [args.market] if args.market in OPTIONS_REGIONS else []

    total = 0

    # ── Cash ──────────────────────────────────────────────────
    if args.dtype in ("cash", "all"):
        for m in cash_markets:
            try:
                n = load_cash(m)
                total += n
            except Exception as e:
                LOG.error(f"Failed loading {m}_cash: {e}")

    # ── Options ───────────────────────────────────────────────
    if args.dtype in ("options", "all"):
        for m in opt_markets:
            try:
                n = load_options(m)
                total += n
            except Exception as e:
                LOG.error(f"Failed loading {m}_options: {e}")

    LOG.info(f"DONE — {total:,} total rows loaded")


if __name__ == "__main__":
    main()