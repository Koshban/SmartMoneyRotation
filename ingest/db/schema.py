"""
src/db/schema.py

Single source of truth for all DB table definitions.

Usage:
    python src/db/schema.py create          # Create all tables
    python src/db/schema.py drop --yes      # Drop all tables (confirm required)
    python src/db/schema.py recreate --yes  # Drop + Create
    python src/db/schema.py status          # Show which tables exist
    python src/db/schema.py drop-options --yes  # Drop only options tables
"""

import argparse
import logging
import sys
from pathlib import Path

import psycopg2

SRC = Path(__file__).resolve().parent.parent
ROOT = SRC.parent
sys.path.insert(0, str(ROOT))

from common.credentials import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)-20s %(levelname)-10s %(message)s",
)
LOG = logging.getLogger(__name__)


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
#  CASH TABLE DDL  (unchanged from your current schema)
# ═══════════════════════════════════════════════════════════════

CASH_REGIONS = ["us", "hk", "in", "others"]

def _cash_ddl(region: str) -> str:
    """
    Cash (equity/ETF) OHLCV table.

    Columns:  date, symbol, open, high, low, close, volume
    Unique:   (date, symbol)
    Index:    symbol, date
    """
    table = f"{region}_cash"
    return f"""
    CREATE TABLE IF NOT EXISTS {table} (
        id          SERIAL PRIMARY KEY,
        date        DATE           NOT NULL,
        symbol      VARCHAR(20)    NOT NULL,
        open        NUMERIC(14,4),
        high        NUMERIC(14,4),
        low         NUMERIC(14,4),
        close       NUMERIC(14,4)  NOT NULL,
        volume      BIGINT,
        created_at  TIMESTAMP DEFAULT NOW(),

        CONSTRAINT uq_{table}_date_symbol
            UNIQUE (date, symbol)
    );

    CREATE INDEX IF NOT EXISTS ix_{table}_symbol
        ON {table} (symbol);

    CREATE INDEX IF NOT EXISTS ix_{table}_date
        ON {table} (date);

    CREATE INDEX IF NOT EXISTS ix_{table}_symbol_date
        ON {table} (symbol, date DESC);
    """


# ═══════════════════════════════════════════════════════════════
#  OPTIONS TABLE DDL  (comprehensive — greeks, bid/ask, source)
# ═══════════════════════════════════════════════════════════════

OPTIONS_REGIONS = ["us", "hk"]  # Add "india" when ready

def _options_ddl(region: str) -> str:
    """
    Options snapshot table — one row per (date, symbol, expiry, strike, opt_type).

    Designed to hold data from both yfinance (no greeks) and IBKR (full greeks).
    Columns that a source doesn't provide are simply NULL.

    Columns:
        Identification:  date, symbol, expiry, strike, opt_type
        Market data:     bid, ask, last, volume, oi
        Volatility:      iv
        Greeks:          delta, gamma, theta, vega, rho
        Context:         underlying_price, dte
        Metadata:        source, created_at

    Unique:  (date, symbol, expiry, strike, opt_type)
    """
    table = f"{region}_options"
    return f"""
    CREATE TABLE IF NOT EXISTS {table} (
        id                SERIAL PRIMARY KEY,

        -- ── Identification ────────────────────────────────────
        date              DATE           NOT NULL,
        symbol            VARCHAR(20)    NOT NULL,
        expiry            DATE           NOT NULL,
        strike            NUMERIC(14,4)  NOT NULL,
        opt_type          CHAR(1)        NOT NULL CHECK (opt_type IN ('C', 'P')),

        -- ── Market Data ───────────────────────────────────────
        bid               NUMERIC(14,4),
        ask               NUMERIC(14,4),
        last              NUMERIC(14,4),
        volume            INTEGER,
        oi                INTEGER,

        -- ── Implied Volatility ────────────────────────────────
        iv                NUMERIC(10,6),

        -- ── Greeks (NULL when source is yfinance) ─────────────
        delta             NUMERIC(10,6),
        gamma             NUMERIC(10,6),
        theta             NUMERIC(10,6),
        vega              NUMERIC(10,6),
        rho               NUMERIC(10,6),

        -- ── Context ──────────────────────────────────────────
        underlying_price  NUMERIC(14,4),
        dte               INTEGER,

        -- ── Metadata ─────────────────────────────────────────
        source            VARCHAR(20)    DEFAULT 'yfinance',
        created_at        TIMESTAMP      DEFAULT NOW(),

        -- ── Constraints ──────────────────────────────────────
        CONSTRAINT uq_{table}_snapshot
            UNIQUE (date, symbol, expiry, strike, opt_type)
    );

    -- Fast lookups by symbol
    CREATE INDEX IF NOT EXISTS ix_{table}_symbol
        ON {table} (symbol);

    -- Fast lookups by date (for daily snapshots)
    CREATE INDEX IF NOT EXISTS ix_{table}_date
        ON {table} (date);

    -- Composite: symbol + date (most common query pattern)
    CREATE INDEX IF NOT EXISTS ix_{table}_symbol_date
        ON {table} (symbol, date DESC);

    -- Composite: symbol + expiry (for chain lookups)
    CREATE INDEX IF NOT EXISTS ix_{table}_symbol_expiry
        ON {table} (symbol, expiry);

    -- Filtered: high-IV contracts
    CREATE INDEX IF NOT EXISTS ix_{table}_iv
        ON {table} (iv DESC)
        WHERE iv IS NOT NULL;

    -- Filtered: IBKR data with greeks
    CREATE INDEX IF NOT EXISTS ix_{table}_greeks
        ON {table} (symbol, expiry, strike)
        WHERE delta IS NOT NULL;
    """


# ═══════════════════════════════════════════════════════════════
#  AGGREGATE / DERIVED TABLE  (optional — for pipeline output)
# ═══════════════════════════════════════════════════════════════

def _signals_ddl() -> str:
    """
    Pipeline output: daily signals / scores per ticker.

    This table is OPTIONAL. The pipeline can write to parquet instead.
    Kept here so the DB can serve as a single reporting layer.
    """
    return """
    CREATE TABLE IF NOT EXISTS signals (
        id              SERIAL PRIMARY KEY,
        date            DATE           NOT NULL,
        symbol          VARCHAR(20)    NOT NULL,
        market          VARCHAR(10)    NOT NULL,

        -- ── Cash Metrics ──────────────────────────────────────
        close           NUMERIC(14,4),
        rsi_14          NUMERIC(8,4),
        macd            NUMERIC(14,6),
        macd_signal     NUMERIC(14,6),
        bb_pct          NUMERIC(8,4),
        atr_14          NUMERIC(14,4),
        adx_14          NUMERIC(8,4),
        vol_z_20        NUMERIC(8,4),

        -- ── Options Metrics ───────────────────────────────────
        iv_avg          NUMERIC(10,6),
        iv_skew         NUMERIC(10,6),
        put_call_ratio  NUMERIC(8,4),
        max_oi_strike   NUMERIC(14,4),
        total_oi        INTEGER,
        total_volume    INTEGER,

        -- ── Scores ────────────────────────────────────────────
        cash_score      NUMERIC(8,4),
        options_score   NUMERIC(8,4),
        combined_score  NUMERIC(8,4),
        regime          VARCHAR(20),
        recommendation  VARCHAR(50),

        -- ── Metadata ─────────────────────────────────────────
        created_at      TIMESTAMP DEFAULT NOW(),

        CONSTRAINT uq_signals_date_symbol
            UNIQUE (date, symbol)
    );

    CREATE INDEX IF NOT EXISTS ix_signals_date
        ON signals (date DESC);

    CREATE INDEX IF NOT EXISTS ix_signals_symbol
        ON signals (symbol);

    CREATE INDEX IF NOT EXISTS ix_signals_score
        ON signals (combined_score DESC)
        WHERE combined_score IS NOT NULL;
    """


# ═══════════════════════════════════════════════════════════════
#  REGISTRY — all tables managed by this schema
# ═══════════════════════════════════════════════════════════════

def all_table_names() -> list[str]:
    """Every table this schema manages, in creation order."""
    tables = [f"{r}_cash" for r in CASH_REGIONS]
    tables += [f"{r}_options" for r in OPTIONS_REGIONS]
    tables.append("signals")
    return tables


def all_ddl() -> list[str]:
    """All DDL statements in creation order."""
    stmts = [_cash_ddl(r) for r in CASH_REGIONS]
    stmts += [_options_ddl(r) for r in OPTIONS_REGIONS]
    stmts.append(_signals_ddl())
    return stmts


def options_table_names() -> list[str]:
    return [f"{r}_options" for r in OPTIONS_REGIONS]


# ═══════════════════════════════════════════════════════════════
#  OPERATIONS
# ═══════════════════════════════════════════════════════════════

def create_all():
    """Create all tables (idempotent — IF NOT EXISTS)."""
    conn = get_conn()
    cur = conn.cursor()
    try:
        for ddl in all_ddl():
            cur.execute(ddl)
        conn.commit()
        LOG.info(f"Created tables: {', '.join(all_table_names())}")
    except Exception as e:
        conn.rollback()
        LOG.error(f"Create failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def drop_all():
    """Drop ALL managed tables. Destructive!"""
    conn = get_conn()
    cur = conn.cursor()
    try:
        for table in reversed(all_table_names()):
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
            LOG.info(f"  Dropped: {table}")
        conn.commit()
    except Exception as e:
        conn.rollback()
        LOG.error(f"Drop failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def drop_options():
    """Drop only options tables. Preserves cash data."""
    conn = get_conn()
    cur = conn.cursor()
    try:
        for table in options_table_names():
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
            LOG.info(f"  Dropped: {table}")
        conn.commit()
    except Exception as e:
        conn.rollback()
        LOG.error(f"Drop failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def table_status() -> dict[str, dict]:
    """Check which tables exist and their row counts."""
    conn = get_conn()
    cur = conn.cursor()
    status = {}
    try:
        for table in all_table_names():
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = %s
                );
            """, (table,))
            exists = cur.fetchone()[0]

            rows = 0
            if exists:
                cur.execute(f"SELECT COUNT(*) FROM {table};")
                rows = cur.fetchone()[0]

            status[table] = {"exists": exists, "rows": rows}
    finally:
        cur.close()
        conn.close()

    return status


def print_status():
    """Pretty-print table status."""
    status = table_status()
    LOG.info("=" * 50)
    LOG.info(f"{'Table':<20s} {'Exists':<10s} {'Rows':>10s}")
    LOG.info("-" * 50)
    for table, info in status.items():
        marker = "✓" if info["exists"] else "✗"
        rows = f"{info['rows']:,}" if info["exists"] else "—"
        LOG.info(f"{table:<20s} {marker:<10s} {rows:>10s}")
    LOG.info("=" * 50)


# ═══════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Manage DB schema for options pipeline",
    )
    parser.add_argument(
        "action",
        choices=["create", "drop", "recreate", "status", "drop-options"],
        help="Action to perform",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Required for destructive operations (drop, recreate)",
    )
    args = parser.parse_args()

    if args.action == "create":
        create_all()

    elif args.action == "drop":
        if not args.yes:
            LOG.error("Pass --yes to confirm dropping ALL tables")
            return
        drop_all()

    elif args.action == "recreate":
        if not args.yes:
            LOG.error("Pass --yes to confirm drop + recreate ALL tables")
            return
        drop_all()
        create_all()

    elif args.action == "drop-options":
        if not args.yes:
            LOG.error("Pass --yes to confirm dropping options tables")
            return
        drop_options()
        LOG.info("Now run: python src/db/schema.py create")

    elif args.action == "status":
        print_status()


if __name__ == "__main__":
    main()