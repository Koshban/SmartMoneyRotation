"""
ingest/db/load_db.py — Load parquet/CSV data into PostgreSQL (upsert).

Uses INSERT ... ON CONFLICT DO UPDATE so the script is safe to
re-run at any time.  Duplicate rows (by the table's unique key)
are updated in place rather than rejected.

Usage:
    python ingest/db/load_db.py                            # all markets, all types
    python ingest/db/load_db.py --market us                 # US only
    python ingest/db/load_db.py --type cash                 # cash tables only
    python ingest/db/load_db.py --type options              # options tables only
    python ingest/db/load_db.py --market us --type cash     # US cash only
    python ingest/db/load_db.py --dry-run                   # preview, no DB writes
    python ingest/db/load_db.py --status                    # show row counts only
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