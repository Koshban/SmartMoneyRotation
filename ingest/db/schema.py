"""
ingest/db/schema.py

Single source of truth for all DB table definitions.

Usage:
    python ingest/db/schema.py create              # Create all tables
    python ingest/db/schema.py drop --yes          # Drop ALL tables
    python ingest/db/schema.py recreate --yes      # Drop + Create ALL
    python ingest/db/schema.py status              # Show which tables exist
    python ingest/db/schema.py drop-options --yes  # Drop only {region}_options
    python ingest/db/schema.py drop-derived --yes  # Drop rebuildable feature tables
    python ingest/db/schema.py drop-state --yes    # Drop positions + position_legs
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
#  REGION SETS
# ═══════════════════════════════════════════════════════════════

CASH_REGIONS    = ["us", "hk", "in", "others"]
OPTIONS_REGIONS = ["us", "hk"]   # add "in" when ready

# Allowed market codes (for documentation; enforced by callers, not DDL)
MARKET_CODES = {"us", "hk", "in"}


# ═══════════════════════════════════════════════════════════════
#  CASH TABLE DDL
# ═══════════════════════════════════════════════════════════════

def _cash_ddl(region: str) -> str:
    """
    Cash (equity/ETF) OHLCV table.

    Columns:  date, symbol, open, high, low, close, volume
    Unique:   (date, symbol)
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
#  OPTIONS SNAPSHOT DDL  (one row per contract per day)
# ═══════════════════════════════════════════════════════════════

def _options_ddl(region: str) -> str:
    """
    Raw options snapshot — one row per (date, symbol, expiry, strike, opt_type).

    `source` is metadata only. Ingest logic uses ON CONFLICT to UPSERT:
    typically IBKR row overwrites yfinance row when both are present.

    `greeks_source` tells you how the greeks were obtained:
        'ibkr_model'  — directly from IBKR modelGreeks
        'computed_bs' — computed via Black-Scholes from iv + underlying_price
        NULL          — greeks not populated
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

        -- ── Greeks ────────────────────────────────────────────
        delta             NUMERIC(10,6),
        gamma             NUMERIC(10,6),
        theta             NUMERIC(10,6),
        vega              NUMERIC(10,6),
        rho               NUMERIC(10,6),

        -- ── Context ──────────────────────────────────────────
        underlying_price  NUMERIC(14,4),
        dte               INTEGER,

        -- ── Metadata ─────────────────────────────────────────
        source            VARCHAR(20)    NOT NULL DEFAULT 'yfinance',
        greeks_source     VARCHAR(20),    -- 'ibkr_model' | 'computed_bs' | NULL
        created_at        TIMESTAMP      DEFAULT NOW(),
        updated_at        TIMESTAMP      DEFAULT NOW(),

        CONSTRAINT uq_{table}_snapshot
            UNIQUE (date, symbol, expiry, strike, opt_type)
    );

    CREATE INDEX IF NOT EXISTS ix_{table}_symbol
        ON {table} (symbol);

    CREATE INDEX IF NOT EXISTS ix_{table}_date
        ON {table} (date);

    CREATE INDEX IF NOT EXISTS ix_{table}_symbol_date
        ON {table} (symbol, date DESC);

    CREATE INDEX IF NOT EXISTS ix_{table}_symbol_expiry
        ON {table} (symbol, expiry);

    CREATE INDEX IF NOT EXISTS ix_{table}_iv
        ON {table} (iv DESC)
        WHERE iv IS NOT NULL;

    CREATE INDEX IF NOT EXISTS ix_{table}_greeks
        ON {table} (symbol, expiry, strike)
        WHERE delta IS NOT NULL;
    """


# ═══════════════════════════════════════════════════════════════
#  UNDERLYING DAILY FEATURES  (regional)
# ═══════════════════════════════════════════════════════════════

def _underlying_daily_ddl(region: str) -> str:
    """
    Per-day per-symbol feature row. The options engine joins on
    (date, symbol) to read trend, vol, S/R levels, and the cash signal.

    OHLCV is denormalized from {region}_cash for join speed.
    """
    table = f"{region}_underlying_daily"
    return f"""
    CREATE TABLE IF NOT EXISTS {table} (
        date                     DATE          NOT NULL,
        symbol                   VARCHAR(20)   NOT NULL,

        -- ── OHLCV (denormalized from {region}_cash) ──────────
        open                     NUMERIC(14,4),
        high                     NUMERIC(14,4),
        low                      NUMERIC(14,4),
        close                    NUMERIC(14,4),
        volume                   BIGINT,

        -- ── Trend / momentum ──────────────────────────────────
        sma_20                   NUMERIC(14,4),
        sma_50                   NUMERIC(14,4),
        sma_100                  NUMERIC(14,4),
        sma_200                  NUMERIC(14,4),
        ema_20                   NUMERIC(14,4),
        rsi_14                   NUMERIC(8,4),
        adx_14                   NUMERIC(8,4),
        macd                     NUMERIC(10,4),
        macd_signal              NUMERIC(10,4),

        -- ── Volatility ────────────────────────────────────────
        atr_14                   NUMERIC(14,4),
        atr_20                   NUMERIC(14,4),
        rv_20                    NUMERIC(10,6),
        rv_60                    NUMERIC(10,6),
        rv_252                   NUMERIC(10,6),

        -- ── Support / Resistance (rolling pivots) ─────────────
        support_20               NUMERIC(14,4),
        resistance_20            NUMERIC(14,4),
        support_60               NUMERIC(14,4),
        resistance_60            NUMERIC(14,4),

        -- ── Cash-market signal (from your existing system) ────
        cash_signal              VARCHAR(20),
        cash_score               NUMERIC(8,4),
        fair_buy_zone_lo         NUMERIC(14,4),
        fair_buy_zone_hi         NUMERIC(14,4),

        -- ── Event proximity (denormalized for fast filtering) ─
        earnings_in_next_n_days  INTEGER,
        exdiv_in_next_n_days     INTEGER,

        -- ── Quality rubric (your taxonomy) ───────────────────
        quality_rank             VARCHAR(10),

        created_at               TIMESTAMP DEFAULT NOW(),

        CONSTRAINT pk_{table}
            PRIMARY KEY (date, symbol)
    );

    CREATE INDEX IF NOT EXISTS ix_{table}_symbol_date
        ON {table} (symbol, date DESC);

    CREATE INDEX IF NOT EXISTS ix_{table}_signal
        ON {table} (date, cash_signal)
        WHERE cash_signal IS NOT NULL;
    """


# ═══════════════════════════════════════════════════════════════
#  IV HISTORY  (regional)  —  for IV rank / IV percentile
# ═══════════════════════════════════════════════════════════════

def _iv_history_ddl(region: str) -> str:
    """
    Per-day per-symbol per-tenor ATM IV. Computed at ingest from chain.

    tenor_bucket: 'M1' or 'M2' — the two nearest 3rd-Friday expiries
                  with 0 < DTE ≤ 70. No interpolation; `dte` records
                  the actual DTE of the chain used.
    """
    table = f"{region}_iv_history"
    return f"""
    CREATE TABLE IF NOT EXISTS {table} (
        date              DATE         NOT NULL,
        symbol            VARCHAR(20)  NOT NULL,
        tenor_bucket      VARCHAR(4)   NOT NULL,
        dte               INT          NOT NULL,
        atm_iv            NUMERIC,
        atm_iv_call       NUMERIC,
        atm_iv_put        NUMERIC,
        skew_25d          NUMERIC,
        term_slope_m1_m2  NUMERIC,
        CONSTRAINT ck_{table}_tenor CHECK (tenor_bucket IN ('M1','M2')),
        CONSTRAINT pk_{table} PRIMARY KEY (date, symbol, tenor_bucket)
    );

    CREATE INDEX IF NOT EXISTS ix_{table}_symbol_date
        ON {table} (symbol, date DESC);
    """


# ═══════════════════════════════════════════════════════════════
#  OPTION FACTORS  (regional, derived per contract per day)
# ═══════════════════════════════════════════════════════════════

def _option_factors_ddl(region: str) -> str:
    """
    Per-contract derived features. Greeks/IV/OI are denormalized from the
    snapshot row so the engine reads from one table only.
    """
    table = f"{region}_option_factors"
    return f"""
    CREATE TABLE IF NOT EXISTS {table} (
        date                     DATE          NOT NULL,
        symbol                   VARCHAR(20)   NOT NULL,
        expiry                   DATE          NOT NULL,
        strike                   NUMERIC(14,4) NOT NULL,
        opt_type                 CHAR(1)       NOT NULL CHECK (opt_type IN ('C','P')),

        -- ── Pricing basics ────────────────────────────────────
        mid                      NUMERIC(14,4),
        spread                   NUMERIC(14,4),
        spread_pct               NUMERIC(10,6),

        -- ── Economics ─────────────────────────────────────────
        underlying_price         NUMERIC(14,4),
        dte                      INTEGER,
        moneyness_pct            NUMERIC(10,6),
        break_even               NUMERIC(14,4),
        cash_secured             NUMERIC(14,4),
        premium_yield            NUMERIC(10,6),
        annualized_yield         NUMERIC(10,6),
        premium_per_day          NUMERIC(14,6),

        -- ── Risk buffers ──────────────────────────────────────
        distance_to_strike_pct   NUMERIC(10,6),
        distance_to_strike_atr   NUMERIC(10,6),
        support_buffer_pct       NUMERIC(10,6),
        resistance_buffer_pct    NUMERIC(10,6),

        -- ── Greeks (denormalized for fast filtering) ──────────
        delta                    NUMERIC(10,6),
        gamma                    NUMERIC(10,6),
        theta                    NUMERIC(10,6),
        vega                     NUMERIC(10,6),
        abs_delta                NUMERIC(10,6),
        prob_itm_proxy           NUMERIC(10,6),

        -- ── Volatility context ────────────────────────────────
        iv                       NUMERIC(10,6),
        iv_rank_252              NUMERIC(8,4),
        iv_pctile_252            NUMERIC(8,4),
        iv_rv_ratio              NUMERIC(10,6),

        -- ── Liquidity ─────────────────────────────────────────
        oi                       INTEGER,
        volume                   INTEGER,
        liquidity_score          NUMERIC(8,4),

        -- ── Event proximity (denormalized) ────────────────────
        earnings_days            INTEGER,
        exdiv_days               INTEGER,

        created_at               TIMESTAMP DEFAULT NOW(),

        CONSTRAINT pk_{table}
            PRIMARY KEY (date, symbol, expiry, strike, opt_type)
    );

    CREATE INDEX IF NOT EXISTS ix_{table}_date_sym_type
        ON {table} (date, symbol, opt_type);

    CREATE INDEX IF NOT EXISTS ix_{table}_date_abs_delta
        ON {table} (date, abs_delta);

    CREATE INDEX IF NOT EXISTS ix_{table}_sym_exp
        ON {table} (symbol, expiry);
    """


# ═══════════════════════════════════════════════════════════════
#  EVENTS CALENDAR  (global)
# ═══════════════════════════════════════════════════════════════

def _events_calendar_ddl() -> str:
    """
    Earnings, ex-div, splits, mergers, FOMC. Used as a hard pre-trade filter
    and as a feature ('days until next earnings').
    """
    return """
    CREATE TABLE IF NOT EXISTS events_calendar (
        symbol      VARCHAR(20)  NOT NULL,
        market      VARCHAR(10)  NOT NULL,          -- 'us', 'hk', 'in'
        event_date  DATE         NOT NULL,
        event_type  VARCHAR(20)  NOT NULL,          -- EARNINGS, EXDIV, SPLIT, MERGER, FOMC
        source      VARCHAR(20)  NOT NULL DEFAULT 'yfinance',
        confirmed   BOOLEAN      DEFAULT FALSE,
        payload     JSONB,                          -- estimate, dividend amount, ratio, etc.
        created_at  TIMESTAMP    DEFAULT NOW(),

        CONSTRAINT pk_events_calendar
            PRIMARY KEY (symbol, event_date, event_type, source)
    );

    CREATE INDEX IF NOT EXISTS ix_events_calendar_date
        ON events_calendar (event_date);

    CREATE INDEX IF NOT EXISTS ix_events_calendar_market_date
        ON events_calendar (market, event_date);
    """


# ═══════════════════════════════════════════════════════════════
#  LEGACY SIGNALS TABLE  (unchanged — daily summary per ticker)
# ═══════════════════════════════════════════════════════════════

def _signals_ddl() -> str:
    """
    Legacy daily summary table (cash + options metrics). Kept for backward
    compatibility. New per-contract scoring lives in `signal_candidates`.
    """
    return """
    CREATE TABLE IF NOT EXISTS signals (
        id              SERIAL PRIMARY KEY,
        date            DATE           NOT NULL,
        symbol          VARCHAR(20)    NOT NULL,
        market          VARCHAR(10)    NOT NULL,    -- 'us', 'hk', 'in'

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
#  SIGNAL CANDIDATES  (global, per-contract scored output)
# ═══════════════════════════════════════════════════════════════

def _signal_candidates_ddl() -> str:
    """
    Per-contract scored candidate. One row per
    (date, symbol, expiry, strike, opt_type, strategy).
    """
    return """
    CREATE TABLE IF NOT EXISTS signal_candidates (
        date              DATE          NOT NULL,
        symbol            VARCHAR(20)   NOT NULL,
        market            VARCHAR(10)   NOT NULL,    -- 'us','hk','in'
        expiry            DATE          NOT NULL,
        strike            NUMERIC(14,4) NOT NULL,
        opt_type          CHAR(1)       NOT NULL,
        strategy          VARCHAR(20)   NOT NULL,    -- CSP, CC, NAKED_CALL

        -- ── Scoring ───────────────────────────────────────────
        underlying_score  NUMERIC(8,4),
        contract_score    NUMERIC(8,4),
        final_score       NUMERIC(8,4),

        -- ── Pass/fail breakdown (explainability) ──────────────
        passed_quality    BOOLEAN,
        passed_trend      BOOLEAN,
        passed_iv         BOOLEAN,
        passed_delta      BOOLEAN,
        passed_dte        BOOLEAN,
        passed_liquidity  BOOLEAN,
        passed_event      BOOLEAN,
        passed_buffer     BOOLEAN,

        -- ── Rationale (debuggable, schema-flexible) ───────────
        reasons           JSONB,
        rejected_reasons  JSONB,

        -- ── Suggested execution ───────────────────────────────
        suggested_qty     INTEGER,
        suggested_limit   NUMERIC(14,4),

        engine_version    VARCHAR(20),
        created_at        TIMESTAMP DEFAULT NOW(),

        CONSTRAINT pk_signal_candidates
            PRIMARY KEY (date, symbol, expiry, strike, opt_type, strategy)
    );

    CREATE INDEX IF NOT EXISTS ix_signal_candidates_date_score
        ON signal_candidates (date, final_score DESC);

    CREATE INDEX IF NOT EXISTS ix_signal_candidates_strategy_date
        ON signal_candidates (strategy, date DESC);

    CREATE INDEX IF NOT EXISTS ix_signal_candidates_market_date
        ON signal_candidates (market, date DESC);
    """


# ═══════════════════════════════════════════════════════════════
#  POSITIONS  (global, lifecycle header)
# ═══════════════════════════════════════════════════════════════

def _positions_ddl() -> str:
    """
    Lifecycle header. State transitions:
        OPEN_PUT → ASSIGNED_LONG → OPEN_CALL → CLOSED
                ↘ CLOSED (BTC or expire worthless)
    """
    return """
    CREATE TABLE IF NOT EXISTS positions (
        position_id         BIGSERIAL PRIMARY KEY,
        symbol              VARCHAR(20)  NOT NULL,
        market              VARCHAR(10)  NOT NULL,    -- 'us','hk','in'
        strategy            VARCHAR(20)  NOT NULL,    -- CSP, CC, NAKED_CALL, WHEEL
        state               VARCHAR(20)  NOT NULL,    -- OPEN_PUT, ASSIGNED_LONG, OPEN_CALL, CLOSED

        opened_date         DATE         NOT NULL,
        closed_date         DATE,

        -- ── Initial leg ──────────────────────────────────────
        initial_strike      NUMERIC(14,4),
        initial_expiry      DATE,
        initial_credit      NUMERIC(14,4),

        -- ── Lifecycle aggregates ─────────────────────────────
        cumulative_premium  NUMERIC(14,4) DEFAULT 0,
        cumulative_fees     NUMERIC(14,4) DEFAULT 0,
        roll_count          INTEGER       DEFAULT 0,

        -- ── Stock leg (if assigned) ──────────────────────────
        shares_held         INTEGER       DEFAULT 0,
        avg_share_cost      NUMERIC(14,4),
        cost_basis_adj      NUMERIC(14,4),     -- avg_share_cost − cumulative_premium/100

        -- ── PnL ──────────────────────────────────────────────
        realized_pnl        NUMERIC(14,4) DEFAULT 0,
        unrealized_pnl      NUMERIC(14,4),
        last_marked_date    DATE,

        notes               TEXT,
        created_at          TIMESTAMP DEFAULT NOW(),
        updated_at          TIMESTAMP DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS ix_positions_state    ON positions (state);
    CREATE INDEX IF NOT EXISTS ix_positions_symbol   ON positions (symbol);
    CREATE INDEX IF NOT EXISTS ix_positions_market   ON positions (market);
    CREATE INDEX IF NOT EXISTS ix_positions_open     ON positions (state) WHERE state <> 'CLOSED';
    """


# ═══════════════════════════════════════════════════════════════
#  POSITION LEGS  (global, append-only)
# ═══════════════════════════════════════════════════════════════

def _position_legs_ddl() -> str:
    """
    Every action that happened to a position (option STO/BTC/expire/assign,
    stock buy/sell). Append-only — never update a leg.
    """
    return """
    CREATE TABLE IF NOT EXISTS position_legs (
        leg_id        BIGSERIAL PRIMARY KEY,
        position_id   BIGINT       NOT NULL REFERENCES positions(position_id) ON DELETE CASCADE,
        action        VARCHAR(15)  NOT NULL,
            -- STO, BTC, BTO, STC, EXPIRE, ASSIGN, CALLAWAY, BUY_STOCK, SELL_STOCK
        date          DATE         NOT NULL,

        -- ── Option fields (NULL for stock-only actions) ──────
        expiry        DATE,
        strike        NUMERIC(14,4),
        opt_type      CHAR(1),

        qty           INTEGER      NOT NULL,    -- contracts or shares
        price         NUMERIC(14,4),
        fees          NUMERIC(10,4) DEFAULT 0,

        -- ── Provenance back to the candidate ─────────────────
        signal_date   DATE,
        notes         TEXT,
        created_at    TIMESTAMP DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS ix_position_legs_pos    ON position_legs (position_id);
    CREATE INDEX IF NOT EXISTS ix_position_legs_date   ON position_legs (date);
    CREATE INDEX IF NOT EXISTS ix_position_legs_action ON position_legs (action);
    """


# ═══════════════════════════════════════════════════════════════
#  REGISTRY — categorized for granular ops
# ═══════════════════════════════════════════════════════════════

def cash_table_names() -> list[str]:
    return [f"{r}_cash" for r in CASH_REGIONS]

def options_table_names() -> list[str]:
    return [f"{r}_options" for r in OPTIONS_REGIONS]

def derived_table_names() -> list[str]:
    """Rebuildable feature tables (no permanent loss if dropped)."""
    names  = [f"{r}_underlying_daily" for r in CASH_REGIONS]
    names += [f"{r}_iv_history"       for r in OPTIONS_REGIONS]
    names += [f"{r}_option_factors"   for r in OPTIONS_REGIONS]
    names += ["signal_candidates"]
    return names

def state_table_names() -> list[str]:
    """Trade state — DESTRUCTIVE to drop. Order matters: legs FK-> positions."""
    return ["position_legs", "positions"]

def shared_table_names() -> list[str]:
    return ["events_calendar", "signals"]


def all_table_names() -> list[str]:
    """Every table this schema manages, in CREATION order."""
    out  = cash_table_names()
    out += options_table_names()
    out += [f"{r}_underlying_daily" for r in CASH_REGIONS]
    out += [f"{r}_iv_history"       for r in OPTIONS_REGIONS]
    out += ["events_calendar"]
    out += [f"{r}_option_factors"   for r in OPTIONS_REGIONS]
    out += ["signals", "signal_candidates"]
    out += ["positions", "position_legs"]
    return out


def all_ddl() -> list[str]:
    """All DDL in CREATION order (FK targets must exist first)."""
    stmts  = [_cash_ddl(r)              for r in CASH_REGIONS]
    stmts += [_options_ddl(r)           for r in OPTIONS_REGIONS]
    stmts += [_underlying_daily_ddl(r)  for r in CASH_REGIONS]
    stmts += [_iv_history_ddl(r)        for r in OPTIONS_REGIONS]
    stmts += [_events_calendar_ddl()]
    stmts += [_option_factors_ddl(r)    for r in OPTIONS_REGIONS]
    stmts += [_signals_ddl(), _signal_candidates_ddl()]
    stmts += [_positions_ddl(), _position_legs_ddl()]
    return stmts


# ═══════════════════════════════════════════════════════════════
#  OPERATIONS
# ═══════════════════════════════════════════════════════════════

def _drop_tables(tables: list[str]) -> None:
    """Drop a given list of tables in the order provided."""
    conn = get_conn()
    cur = conn.cursor()
    try:
        for table in tables:
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


def create_all():
    """Create all tables (idempotent — IF NOT EXISTS)."""
    conn = get_conn()
    cur = conn.cursor()
    try:
        for ddl in all_ddl():
            cur.execute(ddl)
        conn.commit()
        LOG.info(f"Created {len(all_table_names())} tables.")
        for t in all_table_names():
            LOG.info(f"  ✓ {t}")
    except Exception as e:
        conn.rollback()
        LOG.error(f"Create failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def drop_all():
    """Drop ALL managed tables. Destructive!"""
    _drop_tables(list(reversed(all_table_names())))


def drop_options():
    """Drop only {region}_options snapshot tables. Preserves cash, derived, state."""
    _drop_tables(options_table_names())


def drop_derived():
    """Drop rebuildable feature tables. Safe — recompute from raw data."""
    _drop_tables(derived_table_names())


def drop_state():
    """Drop positions + position_legs. VERY DESTRUCTIVE — wipes trade history."""
    _drop_tables(state_table_names())  # legs first (FK), then positions


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
    """Pretty-print table status, grouped by category."""
    status = table_status()

    groups = [
        ("CASH (raw)",     cash_table_names()),
        ("OPTIONS (raw)",  options_table_names()),
        ("DERIVED",        derived_table_names()),
        ("SHARED",         shared_table_names()),
        ("STATE",          list(reversed(state_table_names()))),  # display positions first
    ]

    LOG.info("=" * 60)
    LOG.info(f"{'Table':<32s} {'Exists':<8s} {'Rows':>15s}")
    LOG.info("=" * 60)
    for group_name, tables in groups:
        LOG.info(f"-- {group_name} " + "-" * (60 - len(group_name) - 4))
        for table in tables:
            info = status.get(table, {"exists": False, "rows": 0})
            marker = "✓" if info["exists"] else "✗"
            rows = f"{info['rows']:,}" if info["exists"] else "—"
            LOG.info(f"{table:<32s} {marker:<8s} {rows:>15s}")
    LOG.info("=" * 60)


# ═══════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Manage DB schema for options pipeline",
    )
    parser.add_argument(
        "action",
        choices=[
            "create",
            "drop",
            "recreate",
            "status",
            "drop-options",
            "drop-derived",
            "drop-state",
        ],
        help="Action to perform",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Required for destructive operations",
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
            LOG.error("Pass --yes to confirm dropping {region}_options tables")
            return
        drop_options()
        LOG.info("Now run: python ingest/db/schema.py create")

    elif args.action == "drop-derived":
        if not args.yes:
            LOG.error("Pass --yes to confirm dropping derived feature tables")
            return
        drop_derived()
        LOG.info("Now run: python ingest/db/schema.py create")

    elif args.action == "drop-state":
        if not args.yes:
            LOG.error("Pass --yes to confirm dropping positions + position_legs (DESTRUCTIVE)")
            return
        drop_state()
        LOG.info("Now run: python ingest/db/schema.py create")

    elif args.action == "status":
        print_status()


if __name__ == "__main__":
    main()