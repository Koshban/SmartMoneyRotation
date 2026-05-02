"""
ingest/db/loader.py
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
            f"Run: python ingest/ingest_cash.py --market <mkt> --period 2y  "
            f"to backfill, then: python ingest/db/load_db.py --type cash"
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