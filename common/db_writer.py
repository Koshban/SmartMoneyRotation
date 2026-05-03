"""
common/db_writer.py
Shared DB writer for per-region options snapshots.

Targets the tables defined in ingest/db/schema.py:
    us_options, hk_options  (and in_options when added)

Single source of truth for:
  - the canonical options DataFrame schema (OPTIONS_COLS)
  - the upsert SQL (INSERT ... ON CONFLICT)
  - the psycopg2 connection (reuses common.credentials)

All ingest scripts call ``upsert_options(df, market=...)`` and nothing else.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Iterable

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from common.credentials import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema mirror — must stay in sync with ingest/db/schema.py::_options_ddl
# ---------------------------------------------------------------------------
SUPPORTED_MARKETS = {"us", "hk", "in"}

# Canonical column order written to {market}_options.
# Excludes: id (serial), created_at / updated_at (DB defaults).
OPTIONS_COLS: list[str] = [
    # identity (natural key)
    "date",            # date  — snapshot date
    "symbol",          # text
    "expiry",          # date
    "strike",          # numeric
    "opt_type",        # 'C' | 'P'
    # market data
    "bid",
    "ask",
    "last",
    "volume",          # integer
    "oi",              # integer  (open interest)
    # implied vol
    "iv",
    # greeks
    "delta",
    "gamma",
    "theta",
    "vega",
    "rho",
    # context
    "underlying_price",
    "dte",             # integer — days from `date` to `expiry`
    # provenance
    "source",          # 'yfinance' | 'ibkr' | ...
    "greeks_source",   # 'ibkr_model' | 'computed_bs' | NULL
]

NATURAL_KEY = ("date", "symbol", "expiry", "strike", "opt_type")

# Columns updated on conflict — everything except the natural key.
# updated_at is bumped explicitly in the SQL.
_UPDATE_COLS = [c for c in OPTIONS_COLS if c not in NATURAL_KEY]

UNDERLYING_DAILY_COLS = [
    "date", "symbol",
    # OHLCV
    "open", "high", "low", "close", "volume",
    # Trend
    "sma_20", "sma_50", "sma_100", "sma_200",
    "ema_20", "rsi_14", "adx_14", "macd", "macd_signal",
    # Volatility
    "atr_14", "atr_20", "rv_20", "rv_60", "rv_252",
    # S/R
    "support_20", "resistance_20", "support_60", "resistance_60",
    # Cash signal — left NULL by yfinance pipeline; populated by cash system
    "cash_signal", "cash_score", "fair_buy_zone_lo", "fair_buy_zone_hi",
    # Events — left NULL; populated by calendar pipeline
    "earnings_in_next_n_days", "exdiv_in_next_n_days",
    # Quality
    "quality_rank",
]


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------
def _get_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS,
    )


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------
def _table_for(market: str) -> str:
    m = market.upper()
    if m not in SUPPORTED_MARKETS:
        raise ValueError(f"Unsupported market {market!r}; expected one of {SUPPORTED_MARKETS}")
    return f"{m.lower()}_options"


def _build_upsert_sql(table: str) -> str:
    cols_csv = ", ".join(OPTIONS_COLS)
    conflict_cols = ", ".join(NATURAL_KEY)
    update_csv = ", ".join(f"{c} = EXCLUDED.{c}" for c in _UPDATE_COLS)
    return (
        f"INSERT INTO {table} ({cols_csv}) "
        f"VALUES %s "
        f"ON CONFLICT ({conflict_cols}) DO UPDATE "
        f"SET {update_csv}, updated_at = NOW()"
    )


# ---------------------------------------------------------------------------
# DataFrame coercion
# ---------------------------------------------------------------------------
_REQUIRED = {"date", "symbol", "expiry", "strike", "opt_type"}


def _coerce_df(df: pd.DataFrame) -> pd.DataFrame:
    """Add missing columns, normalise types, enforce column order."""
    df = df.copy()

    missing = _REQUIRED - set(df.columns)
    if missing:
        raise ValueError(f"Provider DataFrame missing required cols: {missing}")

    # Fill optional columns with None
    for c in OPTIONS_COLS:
        if c not in df.columns:
            df[c] = None

    # Normalise opt_type to single uppercase char
    df["opt_type"] = df["opt_type"].astype(str).str.upper().str[0]
    bad = ~df["opt_type"].isin(["C", "P"])
    if bad.any():
        raise ValueError(f"Invalid opt_type values: {df.loc[bad, 'opt_type'].unique()}")

    # Compute dte if missing (or null)
    if df["dte"].isna().any():
        d = pd.to_datetime(df["date"]).dt.date
        e = pd.to_datetime(df["expiry"]).dt.date
        df["dte"] = [(ee - dd).days for dd, ee in zip(d, e)]

    # Coerce integer-ish columns: pandas can't insert NaN into INTEGER columns,
    # and Series.where(..., None) silently re-coerces None back to NaN on
    # float dtypes — so check NaN explicitly inside the lambda.
    for c in ("volume", "oi", "dte"):
        df[c] = df[c].apply(
            lambda v: int(v) if v is not None and pd.notna(v) else None
        )

    # Replace any remaining NaN with None for safe psycopg2 binding
    df = df.astype(object).where(pd.notna(df), None)

    return df[OPTIONS_COLS]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def upsert_options(
    df: pd.DataFrame,
    market: str,
    *,
    page_size: int = 1000,
    dry_run: bool = False,
) -> int:
    """
    Upsert an options-chain DataFrame into ``{market}_options``.

    Returns the number of rows written (0 if dry_run).
    Idempotent: re-running the same snapshot updates rows in place.
    """
    if df is None or df.empty:
        log.info("upsert_options: empty DataFrame, nothing to write")
        return 0

    table = _table_for(market)
    df = _coerce_df(df)

    if dry_run:
        log.info(
            "[dry-run] would upsert %d rows into %s",
            len(df), table,
        )
        return 0

    sql = _build_upsert_sql(table)
    rows: Iterable[tuple] = [tuple(r) for r in df.itertuples(index=False, name=None)]

    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                execute_values(cur, sql, rows, page_size=page_size)
        log.info("upserted %d rows into %s", len(df), table)
        return len(df)
    finally:
        conn.close()


def upsert_underlying_daily(df: pd.DataFrame, market: str, dry_run: bool = False, page_size: int = 500) -> int:
    if df is None or df.empty:
        return 0
    table = f"{market.lower()}_underlying_daily"

    # Add missing columns as NULL so partial pipelines (yfinance-only)
    # don't blow up on the column count.
    df = df.copy()
    for col in UNDERLYING_DAILY_COLS:
        if col not in df.columns:
            df[col] = None
    df = df.where(pd.notna(df), None)

    rows = [tuple(r[c] for c in UNDERLYING_DAILY_COLS) for _, r in df.iterrows()]
    cols_sql = ", ".join(UNDERLYING_DAILY_COLS)

    # Only update columns this pipeline owns. Don't clobber cash_signal / events / quality
    # written by other pipelines just because yfinance ran second.
    update_cols = [
        "open", "high", "low", "close", "volume",
        "sma_20", "sma_50", "sma_100", "sma_200",
        "ema_20", "rsi_14", "adx_14", "macd", "macd_signal",
        "atr_14", "atr_20", "rv_20", "rv_60", "rv_252",
        "support_20", "resistance_20", "support_60", "resistance_60",
    ]
    update_sql = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)

    sql = f"""
        INSERT INTO {table} ({cols_sql}) VALUES %s
        ON CONFLICT (date, symbol) DO UPDATE SET
            {update_sql}
    """

    if dry_run:
        log.info(f"[dry-run] would upsert {len(rows)} rows into {table}")
        return len(rows)

    with _get_conn() as conn, conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=page_size)
        conn.commit()
    log.info(f"upserted {len(rows)} rows into {table}")
    return len(rows)