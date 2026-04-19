"""
schema.py – Create PostgreSQL tables for the SmartMoneyRotation project.

Tables per region (us, hk, india, others):
  - {region}_cash       OHLCV for equities / ETFs
  - {region}_options    Options chain snapshots

Run:
    python src/db/schema.py
"""

import logging
import psycopg2
from psycopg2 import sql
import sys
from pathlib import Path
from common.credentials import PG_CONFIG

# Ensure src/ is on the Python path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
DB_CONFIG = PG_CONFIG
REGIONS = ["us", "hk", "india", "others"]


# ====================================================================
#  SQL Templates
# ====================================================================

CASH_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {table_name} (
    symbol          VARCHAR(30)     NOT NULL,
    trade_date      DATE            NOT NULL,
    open            NUMERIC(18,6),
    high            NUMERIC(18,6),
    low             NUMERIC(18,6),
    close           NUMERIC(18,6),
    adj_close       NUMERIC(18,6),
    volume          BIGINT,
    vwap            NUMERIC(18,6),
    num_trades      INTEGER,
    exchange        VARCHAR(10),
    currency        VARCHAR(5)      NOT NULL,
    created_at      TIMESTAMPTZ     DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     DEFAULT NOW(),

    PRIMARY KEY (symbol, trade_date)
);

-- Fast lookups by date range
CREATE INDEX IF NOT EXISTS idx_{table_name}_date
    ON {table_name} (trade_date);

-- Fast lookups by symbol + date range
CREATE INDEX IF NOT EXISTS idx_{table_name}_sym_date
    ON {table_name} (symbol, trade_date DESC);

COMMENT ON TABLE {table_name} IS 'Daily OHLCV cash/equity data – {region} market';
"""


OPTIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {table_name} (
    symbol              VARCHAR(30)     NOT NULL,
    trade_date          DATE            NOT NULL,
    expiry_date         DATE            NOT NULL,
    strike              NUMERIC(18,6)   NOT NULL,
    option_type         VARCHAR(4)      NOT NULL,   -- 'CALL' or 'PUT'
    open                NUMERIC(18,6),
    high                NUMERIC(18,6),
    low                 NUMERIC(18,6),
    close               NUMERIC(18,6),
    last_price          NUMERIC(18,6),
    volume              BIGINT,
    open_interest       BIGINT,
    implied_volatility  NUMERIC(12,6),
    delta               NUMERIC(10,6),
    gamma               NUMERIC(10,6),
    theta               NUMERIC(10,6),
    vega                NUMERIC(10,6),
    rho                 NUMERIC(10,6),
    underlying_price    NUMERIC(18,6),
    bid                 NUMERIC(18,6),
    ask                 NUMERIC(18,6),
    bid_size            INTEGER,
    ask_size            INTEGER,
    exchange            VARCHAR(10),
    currency            VARCHAR(5)      NOT NULL,
    created_at          TIMESTAMPTZ     DEFAULT NOW(),
    updated_at          TIMESTAMPTZ     DEFAULT NOW(),

    PRIMARY KEY (symbol, trade_date, expiry_date, strike, option_type)
);

-- Lookup by underlying + date
CREATE INDEX IF NOT EXISTS idx_{table_name}_sym_date
    ON {table_name} (symbol, trade_date DESC);

-- Lookup by expiry
CREATE INDEX IF NOT EXISTS idx_{table_name}_expiry
    ON {table_name} (expiry_date, symbol);

-- Options chain slice: symbol + expiry + type
CREATE INDEX IF NOT EXISTS idx_{table_name}_chain
    ON {table_name} (symbol, expiry_date, option_type, strike);

COMMENT ON TABLE {table_name} IS 'Options chain snapshots – {region} market';
"""


# ====================================================================
#  Updated-at trigger (auto-set updated_at on row change)
# ====================================================================

TRIGGER_FUNC_SQL = """
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""

TRIGGER_SQL = """
DROP TRIGGER IF EXISTS trg_updated_at ON {table_name};
CREATE TRIGGER trg_updated_at
    BEFORE UPDATE ON {table_name}
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
"""


# ====================================================================
#  Table creation
# ====================================================================

def get_connection():
    """Return a psycopg2 connection using DB_CONFIG."""
    logger.info(f"Connecting to PostgreSQL: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
    return psycopg2.connect(**DB_CONFIG)


def create_tables():
    """Create all cash and options tables for every region."""
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()

    # ── Trigger function (shared) ──────────────────────────────
    logger.info("Creating trigger function: update_updated_at_column()")
    cur.execute(TRIGGER_FUNC_SQL)

    for region in REGIONS:
        # ── Cash table ─────────────────────────────────────────
        cash_table = f"{region}_cash"
        logger.info(f"Creating table: {cash_table}")
        cur.execute(CASH_TABLE_SQL.format(table_name=cash_table, region=region.upper()))
        cur.execute(TRIGGER_SQL.format(table_name=cash_table))

        # ── Options table ──────────────────────────────────────
        opts_table = f"{region}_options"
        logger.info(f"Creating table: {opts_table}")
        cur.execute(OPTIONS_TABLE_SQL.format(table_name=opts_table, region=region.upper()))
        cur.execute(TRIGGER_SQL.format(table_name=opts_table))

    cur.close()
    conn.close()
    logger.info("All tables created successfully")


def drop_tables(confirm: bool = False):
    """Drop all cash and options tables. Use with caution."""
    if not confirm:
        logger.warning("drop_tables called without confirm=True — skipping")
        return

    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()

    for region in REGIONS:
        for suffix in ["cash", "options"]:
            table = f"{region}_{suffix}"
            logger.warning(f"DROPPING table: {table}")
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")

    cur.close()
    conn.close()
    logger.info("All tables dropped")


def list_tables():
    """Print all project tables and their row counts."""
    conn = get_connection()
    cur = conn.cursor()

    print(f"\n{'Table':<25} {'Rows':>12} {'Size':>12}")
    print("─" * 51)

    for region in REGIONS:
        for suffix in ["cash", "options"]:
            table = f"{region}_{suffix}"
            try:
                cur.execute(f"SELECT COUNT(*) FROM {table};")
                count = cur.fetchone()[0]
                cur.execute(f"SELECT pg_size_pretty(pg_total_relation_size('{table}'));")
                size = cur.fetchone()[0]
                print(f"{table:<25} {count:>12,} {size:>12}")
            except Exception:
                conn.rollback()
                print(f"{table:<25} {'— missing —':>12}")

    print()
    cur.close()
    conn.close()


# ====================================================================
#  CLI
# ====================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Manage SmartMoneyRotation DB schema")
    parser.add_argument(
        "action",
        choices=["create", "drop", "status"],
        help="create = build tables, drop = destroy tables, status = show row counts",
    )
    parser.add_argument(
        "--yes", action="store_true",
        help="Required for drop to actually execute",
    )
    args = parser.parse_args()

    if args.action == "create":
        create_tables()
    elif args.action == "drop":
        drop_tables(confirm=args.yes)
    elif args.action == "status":
        list_tables()