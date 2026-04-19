"""
db.py
-----
PostgreSQL connection pool, schema definition, and helper utilities.
Uses SQLAlchemy Core (no ORM) for performance and clarity.
"""

from datetime import datetime
from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    String, Float, Integer, BigInteger,
    Date, DateTime,
    UniqueConstraint, Index, text,
)
from sqlalchemy.engine import Engine
from common.config import DB_URL


# ── Connection pool (module-level singleton) ───────────────────
_engine: Engine | None = None


def get_engine() -> Engine:
    """Return a shared SQLAlchemy engine (created once, reused)."""
    global _engine
    if _engine is None:
        _engine = create_engine(
            DB_URL,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            echo=False,
        )
    return _engine


# ── Schema ─────────────────────────────────────────────────────
metadata = MetaData()

# ── 1. Raw daily OHLCV bars from IBKR ─────────────────────────
daily_prices = Table(
    "daily_prices",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("symbol", String(20), nullable=False),
    Column("date", Date, nullable=False),
    Column("open", Float),
    Column("high", Float),
    Column("low", Float),
    Column("close", Float),
    Column("volume", BigInteger),
    Column("bar_count", Integer),
    Column("average", Float),
    Column("created_at", DateTime, default=datetime.utcnow),
    UniqueConstraint("symbol", "date", name="uq_daily_prices_symbol_date"),
)
Index("ix_daily_prices_symbol_date", daily_prices.c.symbol, daily_prices.c.date)


# ── 2. Computed technical indicators (one row per symbol/date) ─
indicators = Table(
    "indicators",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("symbol", String(20), nullable=False),
    Column("date", Date, nullable=False),
    # Returns
    Column("return_5d", Float),
    Column("return_10d", Float),
    Column("return_15d", Float),
    # Trend
    Column("ema_30", Float),
    Column("sma_30", Float),
    Column("sma_50", Float),
    Column("price_vs_ema30", Float),
    Column("price_vs_sma50", Float),
    Column("sma30_vs_sma50", Float),
    # Momentum
    Column("rsi_14", Float),
    Column("macd_line", Float),
    Column("macd_signal", Float),
    Column("macd_histogram", Float),
    Column("adx_14", Float),
    # Volatility
    Column("atr_14", Float),
    Column("atr_pct", Float),
    Column("zscore_60", Float),
    # Volume
    Column("obv", Float),
    Column("obv_slope", Float),
    Column("ad_line", Float),
    Column("volume_ratio", Float),
    Column("avg_volume_20", Float),
    # Liquidity
    Column("amihud_illiquidity", Float),
    # Relative strength
    Column("rs_vs_spy", Float),
    Column("correlation_spy_60", Float),
    Column("created_at", DateTime, default=datetime.utcnow),
    UniqueConstraint("symbol", "date", name="uq_indicators_symbol_date"),
)
Index("ix_indicators_symbol_date", indicators.c.symbol, indicators.c.date)


# ── 3. Options-derived metrics ─────────────────────────────────
options_metrics = Table(
    "options_metrics",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("symbol", String(20), nullable=False),
    Column("date", Date, nullable=False),
    Column("iv_rank", Float),
    Column("iv_percentile", Float),
    Column("put_call_ratio", Float),
    Column("call_volume", BigInteger),
    Column("put_volume", BigInteger),
    Column("total_option_volume", BigInteger),
    Column("implied_move", Float),
    Column("skew", Float),
    Column("created_at", DateTime, default=datetime.utcnow),
    UniqueConstraint("symbol", "date", name="uq_options_symbol_date"),
)
Index("ix_options_symbol_date", options_metrics.c.symbol, options_metrics.c.date)


# ── 4. Composite scores and rankings ──────────────────────────
scores = Table(
    "scores",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("symbol", String(20), nullable=False),
    Column("date", Date, nullable=False),
    Column("theme", String(50)),
    # Component scores (0-100 each)
    Column("trend_score", Float),
    Column("momentum_score", Float),
    Column("relative_strength_score", Float),
    Column("volume_accumulation_score", Float),
    Column("breadth_score", Float),
    Column("options_flow_score", Float),
    Column("volatility_score", Float),
    Column("liquidity_score", Float),
    Column("structure_score", Float),
    Column("etf_flows_score", Float),
    # Final
    Column("composite_score", Float),
    Column("rank", Integer),
    Column("created_at", DateTime, default=datetime.utcnow),
    UniqueConstraint("symbol", "date", name="uq_scores_symbol_date"),
)
Index("ix_scores_date_rank", scores.c.date, scores.c.rank)
Index("ix_scores_symbol_date", scores.c.symbol, scores.c.date)


# ── DDL helpers ────────────────────────────────────────────────

def init_db() -> None:
    """Create all tables if they don't exist."""
    engine = get_engine()
    metadata.create_all(engine)
    print("  [OK] All database tables created / verified.")


def drop_all_tables() -> None:
    """Drop all tables (use with caution!)."""
    engine = get_engine()
    metadata.drop_all(engine)
    print("  [OK] All tables dropped.")


def test_connection() -> bool:
    """Quick connectivity check - returns True if DB is reachable."""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            return result.scalar() == 1
    except Exception as e:
        print(f"  [FAIL] DB connection error: {e}")
        return False


# ── CLI entry point ────────────────────────────────────────────
if __name__ == "__main__":
    print("Testing database connection...")
    if test_connection():
        print("[OK] Connection successful.")
        print("Creating tables...")
        init_db()
    else:
        print("[FAIL] Cannot reach database. Check config and PostgreSQL status.")