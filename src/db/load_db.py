"""
load_db.py – Read parquet files and upsert into PostgreSQL (cash + options).

Usage:
    python src/db/load_db.py --market all --type cash
    python src/db/load_db.py --market all --type options
    python src/db/load_db.py --market all --type all
    python src/db/load_db.py --market india --type cash
    python src/db/load_db.py --market us --type options --dry-run
"""

import sys
import logging
import argparse
from pathlib import Path

# ── Ensure src/ is on the Python path ─────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from common.credentials import PG_CONFIG

# ── Paths ──────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
CASH_PARQUET = DATA_DIR / "universe_ohlcv.parquet"

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)

BATCH_SIZE = 5000

# ── Region classification ──────────────────────────────────────
REGION_CONFIG = {
    "us":     {"cash_table": "us_cash",     "options_table": "us_options",     "currency": "USD"},
    "hk":     {"cash_table": "hk_cash",     "options_table": "hk_options",     "currency": "HKD"},
    "india":  {"cash_table": "india_cash",  "options_table": "india_options",  "currency": "INR"},
    "others": {"cash_table": "others_cash", "options_table": "others_options", "currency": "USD"},
}

OPTIONS_PARQUETS = {
    "us": DATA_DIR / "us_options.parquet",
    "hk": DATA_DIR / "hk_options.parquet",
}

SUFFIX_TO_REGION = {
    ".HK": "hk",
    ".NS": "india",
    ".BO": "india",
}


def classify_symbol(symbol: str) -> str:
    """Determine region from ticker suffix."""
    for suffix, region in SUFFIX_TO_REGION.items():
        if symbol.upper().endswith(suffix.upper()):
            return region
    if "." not in symbol:
        return "us"
    return "others"


# ====================================================================
#  SQL
# ====================================================================

CASH_COLUMNS = [
    "symbol", "trade_date", "open", "high", "low",
    "close", "adj_close", "volume", "currency",
]

CASH_UPSERT_SQL = """
INSERT INTO {table} (symbol, trade_date, open, high, low, close, adj_close, volume, currency)
VALUES %s
ON CONFLICT (symbol, trade_date) DO UPDATE SET
    open       = EXCLUDED.open,
    high       = EXCLUDED.high,
    low        = EXCLUDED.low,
    close      = EXCLUDED.close,
    adj_close  = EXCLUDED.adj_close,
    volume     = EXCLUDED.volume,
    currency   = EXCLUDED.currency,
    updated_at = NOW()
;
"""

OPTIONS_COLUMNS = [
    "symbol", "trade_date", "expiry_date", "strike", "option_type",
    "last_price", "volume", "open_interest",
    "implied_volatility", "delta", "gamma", "theta", "vega", "rho",
    "underlying_price", "bid", "ask", "bid_size", "ask_size",
    "exchange", "currency",
]

OPTIONS_UPSERT_SQL = """
INSERT INTO {table} (
    symbol, trade_date, expiry_date, strike, option_type,
    last_price, volume, open_interest,
    implied_volatility, delta, gamma, theta, vega, rho,
    underlying_price, bid, ask, bid_size, ask_size,
    exchange, currency
)
VALUES %s
ON CONFLICT (symbol, trade_date, expiry_date, strike, option_type) DO UPDATE SET
    last_price         = EXCLUDED.last_price,
    volume             = EXCLUDED.volume,
    open_interest      = EXCLUDED.open_interest,
    implied_volatility = EXCLUDED.implied_volatility,
    delta              = EXCLUDED.delta,
    gamma              = EXCLUDED.gamma,
    theta              = EXCLUDED.theta,
    vega               = EXCLUDED.vega,
    rho                = EXCLUDED.rho,
    underlying_price   = EXCLUDED.underlying_price,
    bid                = EXCLUDED.bid,
    ask                = EXCLUDED.ask,
    bid_size           = EXCLUDED.bid_size,
    ask_size           = EXCLUDED.ask_size,
    exchange           = EXCLUDED.exchange,
    currency           = EXCLUDED.currency,
    updated_at         = NOW()
;
"""

CASH_COL_MAP = {
    "Open": "open", "High": "high", "Low": "low",
    "Close": "close", "Adj Close": "adj_close", "Volume": "volume",
}


# ====================================================================
#  Helpers
# ====================================================================

def get_connection():
    logger.info(f"Connecting to PostgreSQL: {PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['dbname']}")
    return psycopg2.connect(**PG_CONFIG)


def df_to_rows(df: pd.DataFrame, columns: list) -> list[tuple]:
    """Convert DataFrame to list of tuples, replacing NaN with None."""
    for col in columns:
        if col not in df.columns:
            df[col] = None
    subset = df[columns].copy()
    subset = subset.where(subset.notna(), None)
    return [tuple(row) for row in subset.itertuples(index=False, name=None)]


def upsert_rows(conn, table: str, upsert_sql: str, rows: list[tuple], dry_run: bool = False):
    total = len(rows)
    label = " [DRY RUN]" if dry_run else ""
    logger.info(f"Upserting {total:,} rows into {table}{label}")

    if dry_run:
        if rows:
            logger.info(f"  Sample: {rows[0]}")
        return

    sql = upsert_sql.format(table=table)
    cur = conn.cursor()
    inserted = 0

    for i in range(0, total, BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        execute_values(cur, sql, batch, page_size=BATCH_SIZE)
        inserted += len(batch)
        logger.info(f"  {inserted:,} / {total:,}")

    conn.commit()
    cur.close()
    logger.info(f"✓ {table}: {total:,} rows upserted")


# ====================================================================
#  Cash loading
# ====================================================================

def read_cash_parquet() -> dict[str, pd.DataFrame]:
    """Read universe_ohlcv.parquet and split by region."""
    if not CASH_PARQUET.exists():
        logger.warning(f"Cash parquet not found: {CASH_PARQUET}")
        return {}

    logger.info(f"Reading {CASH_PARQUET}")
    df = pd.read_parquet(CASH_PARQUET)

    # Normalise date
    if "Date" not in df.columns and df.index.name in ("Date", "date", None):
        df = df.reset_index()
    date_col = next((c for c in df.columns if c.lower() == "date"), None)
    if date_col is None:
        raise ValueError(f"No Date column. Columns: {list(df.columns)}")
    df.rename(columns={date_col: "trade_date"}, inplace=True)
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date

    # Normalise symbol
    sym_col = next((c for c in df.columns if c.lower() in ("symbol", "ticker")), None)
    if sym_col is None:
        raise ValueError(f"No symbol column. Columns: {list(df.columns)}")
    df.rename(columns={sym_col: "symbol"}, inplace=True)

    # Rename OHLCV
    df.rename(columns=CASH_COL_MAP, inplace=True)

    # Split by region
    df["region"] = df["symbol"].apply(classify_symbol)
    region_dfs = {}
    for region in REGION_CONFIG:
        rdf = df[df["region"] == region].copy()
        if not rdf.empty:
            region_dfs[region] = rdf
            logger.info(f"  {region}: {len(rdf):,} rows, {rdf['symbol'].nunique()} symbols")
    return region_dfs


def load_cash(region: str, region_dfs: dict, dry_run: bool = False):
    cfg = REGION_CONFIG[region]
    df = region_dfs.get(region)
    if df is None or df.empty:
        logger.warning(f"No cash data for {region}")
        return

    df = df.copy()
    df["currency"] = cfg["currency"]
    rows = df_to_rows(df, CASH_COLUMNS)

    if not rows:
        return

    conn = get_connection()
    try:
        upsert_rows(conn, cfg["cash_table"], CASH_UPSERT_SQL, rows, dry_run)
    finally:
        conn.close()


# ====================================================================
#  Options loading
# ====================================================================

def read_options_parquet(region: str) -> pd.DataFrame:
    """Read a region's options parquet."""
    path = OPTIONS_PARQUETS.get(region)
    if path is None or not path.exists():
        logger.warning(f"Options parquet not found for {region}: {path}")
        return pd.DataFrame()

    logger.info(f"Reading {path}")
    df = pd.read_parquet(path)

    # Ensure date columns are proper dates
    for col in ("trade_date", "expiry_date"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col]).dt.date

    return df


def load_options(region: str, dry_run: bool = False):
    cfg = REGION_CONFIG.get(region)
    if cfg is None:
        logger.error(f"Unknown region: {region}")
        return

    df = read_options_parquet(region)
    if df.empty:
        logger.warning(f"No options data for {region}")
        return

    df = df.copy()
    if "currency" not in df.columns:
        df["currency"] = cfg["currency"]

    rows = df_to_rows(df, OPTIONS_COLUMNS)
    if not rows:
        return

    logger.info(f"  {region} options: {len(rows):,} rows, "
                f"{df['symbol'].nunique()} symbols, "
                f"{df['expiry_date'].nunique()} expiries")

    conn = get_connection()
    try:
        upsert_rows(conn, cfg["options_table"], OPTIONS_UPSERT_SQL, rows, dry_run)
    finally:
        conn.close()


# ====================================================================
#  CLI
# ====================================================================

def main():
    parser = argparse.ArgumentParser(description="Load parquet data into PostgreSQL")
    parser.add_argument(
        "--market", required=True,
        choices=["us", "hk", "india", "others", "all"],
    )
    parser.add_argument(
        "--type", required=True,
        choices=["cash", "options", "all"],
        help="Which data type to load",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    markets = list(REGION_CONFIG.keys()) if args.market == "all" else [args.market]
    load_types = ["cash", "options"] if args.type == "all" else [args.type]

    # ── Cash ───────────────────────────────────────────────────
    if "cash" in load_types:
        region_dfs = read_cash_parquet()
        for mkt in markets:
            load_cash(mkt, region_dfs, dry_run=args.dry_run)

    # ── Options ────────────────────────────────────────────────
    if "options" in load_types:
        for mkt in markets:
            load_options(mkt, dry_run=args.dry_run)

    logger.info("Done")


if __name__ == "__main__":
    main()