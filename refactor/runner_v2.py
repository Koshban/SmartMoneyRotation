"""refactor/runner_v2.py"""
from __future__ import annotations

from utils.run_logger import RunLogger
from utils.display_results import print_run_summary

import argparse
import logging
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any


import pandas as pd


from common.config import DATA_DIR, LOGS_DIR
from refactor.common.market_config_v2 import get_market_config_v2
from refactor.pipeline_v2 import run_pipeline_v2
from refactor.report_v2 import build_report_v2, to_text_v2


# ── SIGNAL WRITER: optional import ────────────────────────────
try:
    from signal_writer import write_signals as _write_signals
    _HAS_SIGNAL_WRITER = True
except ImportError:
    _HAS_SIGNAL_WRITER = False


logger = logging.getLogger("refactor.runner_v2")


MARKET_PARQUET = {
    "US": "us_cash.parquet",
    "HK": "hk_cash.parquet",
    "IN": "in_cash.parquet",
}
DATE_CANDIDATE_COLS = ("date", "datetime", "timestamp", "dt")
TICKER_CANDIDATE_COLS = ("ticker", "symbol")
BENCHMARK_FALLBACKS = {
    "US": ["SPY", "QQQ", "IWM"],
    "HK": ["2800.HK"],
    "IN": ["NIFTYBEES.NS"],
}


# ═══════════════════════════════════════════════════════════════
#  NEW ── THEMATIC ETF DEFINITIONS
# ═══════════════════════════════════════════════════════════════
# Maps theme name → list of representative ETFs.
# Only ETFs present in the parquet will be used.

THEMATIC_ETF_MAP: dict[str, dict[str, list[str]]] = {
    "US": {
        "Semiconductors":       ["SOXX", "SMH"],
        "AI / Robotics":        ["QTUM", "AIQ", "BOTZ", "ROBO"],
        "Cybersecurity":        ["HACK", "CIBR", "BUG"],
        "Clean Energy":         ["ICLN", "TAN", "QCLN"],
        "Cloud / Software":     ["SKYY", "WCLD", "IGV"],
        "Blockchain / Crypto":  ["BLOK", "BKCH", "BITQ"],
        "Genomics / Biotech":   ["ARKG", "XBI"],
        "Space / Defense":      ["UFO", "ITA"],
        "Internet / China Tech": ["KWEB", "FDN"],
        "Fintech":              ["FINX", "ARKF"],
        "Momentum Factor":      ["MTUM"],
    },
    "HK": {
        "China Tech":           ["3067.HK", "2845.HK"],
        "Semiconductors":       ["3135.HK"],
    },
    "IN": {},
}

# Flat set per market for quick membership checks
THEMATIC_ETF_TICKERS: dict[str, set[str]] = {
    mkt: {
        etf
        for etfs in themes.values()
        for etf in etfs
    }
    for mkt, themes in THEMATIC_ETF_MAP.items()
}

# 11 GICS sector ETFs (reference – used for logging / diagnostics)
SECTOR_ETF_MAP: dict[str, str] = {
    "Technology":              "XLK",
    "Consumer Discretionary":  "XLY",
    "Communication Services":  "XLC",
    "Financials":              "XLF",
    "Healthcare":              "XLV",
    "Industrials":             "XLI",
    "Consumer Staples":        "XLP",
    "Energy":                  "XLE",
    "Utilities":               "XLU",
    "Real Estate":             "XLRE",
    "Materials":               "XLB",
}


# ═══════════════════════════════════════════════════════════════
#  NEW ── THEMATIC FRAME EXTRACTION
# ═══════════════════════════════════════════════════════════════

def _extract_thematic_frames(
    universe_frames: dict[str, pd.DataFrame],
    market: str,
) -> tuple[dict[str, pd.DataFrame], dict[str, list[str]]]:
    """
    Pull thematic-ETF frames out of the already-loaded universe data.

    Returns
    -------
    thematic_frames : {ticker: DataFrame}
        Price frames for every thematic ETF found in the parquet.
    available_map : {theme_name: [tickers]}
        Only themes that have at least one ETF in the data.
    """
    market_upper = market.upper()
    theme_map = THEMATIC_ETF_MAP.get(market_upper, {})

    thematic_frames: dict[str, pd.DataFrame] = {}
    available_map: dict[str, list[str]] = {}
    missing_tickers: list[str] = []

    for theme, tickers in theme_map.items():
        found_in_theme: list[str] = []
        for ticker in tickers:
            if ticker in universe_frames:
                thematic_frames[ticker] = universe_frames[ticker]
                found_in_theme.append(ticker)
            else:
                missing_tickers.append(ticker)
        if found_in_theme:
            available_map[theme] = found_in_theme

    logger.info(
        "Thematic ETFs: market=%s configured_themes=%d available_themes=%d "
        "configured_etfs=%d found_etfs=%d missing=%d",
        market_upper,
        len(theme_map),
        len(available_map),
        sum(len(v) for v in theme_map.values()),
        len(thematic_frames),
        len(missing_tickers),
    )
    if missing_tickers:
        logger.info(
            "Missing thematic ETFs in parquet: %s", sorted(missing_tickers),
        )
    if logger.isEnabledFor(logging.DEBUG):
        for theme, tickers in available_map.items():
            logger.debug("  Theme '%s': %s", theme, tickers)

    return thematic_frames, available_map


# ═══════════════════════════════════════════════════════════════
#  NEW ── PORTFOLIO SELECTION GAP DIAGNOSTICS  (answers Q1)
# ═══════════════════════════════════════════════════════════════

def _log_portfolio_selection_gaps(result: dict[str, Any]) -> None:
    """
    Compare every STRONG_BUY name from the scored table against the
    names actually selected for the portfolio, and log the diff.

    This makes it obvious when a STRONG_BUY is dropped by the
    portfolio constructor (sector cap, liquidity filter, theme cap …).
    """
    # ── locate scored table ────────────────────────────────
    scored_df: pd.DataFrame | None = None
    for key in _SCORED_TABLE_KEYS:
        candidate = result.get(key)
        if isinstance(candidate, pd.DataFrame) and not candidate.empty:
            scored_df = candidate
            break
    if scored_df is None:
        return

    ticker_col = _find_col(scored_df, TICKER_CANDIDATE_COLS)
    signal_col = _find_col(scored_df, _SIGNAL_COL_CANDIDATES)
    if ticker_col is None or signal_col is None:
        return

    # ── all STRONG_BUY from scored table ───────────────────
    sb_mask = scored_df[signal_col].astype(str).str.upper() == "STRONG_BUY"
    all_strong_buy = set(scored_df.loc[sb_mask, ticker_col].astype(str))
    if not all_strong_buy:
        return

    # ── selected portfolio tickers ─────────────────────────
    report = result.get("report_v2", {})
    selected_set = _extract_portfolio_set(report)

    # Also look in the result dict directly
    for key in ("portfolio_tickers", "selected_tickers", "selected_names"):
        extra = result.get(key)
        if isinstance(extra, (list, set, frozenset)):
            selected_set |= {str(t) for t in extra}

    dropped = sorted(all_strong_buy - selected_set)
    if dropped:
        # Build a mini-table of the dropped names for the log
        drop_rows = scored_df[
            scored_df[ticker_col].astype(str).isin(dropped)
        ]
        preview_cols = [
            c for c in [ticker_col, "composite_score", "final_score", "score",
                        "sector", "gics_sector", "avg_dollar_volume", "dollar_volume",
                        "market_cap", signal_col]
            if c in drop_rows.columns
        ]
        logger.warning(
            "Portfolio selection gap: %d STRONG_BUY names were NOT selected "
            "into TOP RECS: %s  "
            "(likely cause: sector/theme concentration cap, liquidity filter, "
            "or max_positions limit in portfolio construction)",
            len(dropped), dropped,
        )
        if preview_cols:
            logger.warning(
                "Dropped STRONG_BUY details:\n%s",
                drop_rows[preview_cols].to_string(index=False),
            )
    else:
        logger.info(
            "Portfolio selection: all %d STRONG_BUY names included in TOP RECS",
            len(all_strong_buy),
        )


def _parse_iso_date(value: str | None) -> date | None:
    if value in (None, ""):
        return None
    return date.fromisoformat(value)



def setup_logging(verbose: bool = False) -> Path:
    log_dir = Path(LOGS_DIR)
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"runner_v2_{ts}.log"
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, mode="w", encoding="utf-8"),
        ],
        force=True,
    )
    return log_file



def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Run refactor v2 pipeline from desktop CLI")
    p.add_argument("--market", default="US", help="US, HK, or IN")
    p.add_argument("--start-date", type=_parse_iso_date, default=None, help="Inclusive start date YYYY-MM-DD")
    p.add_argument("--end-date", type=_parse_iso_date, default=None, help="Inclusive end date YYYY-MM-DD")
    p.add_argument("--parquet-path", default=None, help="Optional explicit parquet file path")
    p.add_argument("--print-report", action="store_true", help="Print plain-text v2 report to stdout")
    p.add_argument("-v", "--verbose", action="store_true")
    return p



def _resolve_parquet_path(market: str, parquet_path: str | None = None) -> Path:
    if parquet_path:
        return Path(parquet_path)
    m = market.upper()
    if m not in MARKET_PARQUET:
        raise ValueError(f"Unknown market {market!r}")
    return Path(DATA_DIR) / MARKET_PARQUET[m]



def _find_date_col(df: pd.DataFrame) -> str:
    for col in DATE_CANDIDATE_COLS:
        if col in df.columns:
            return col
    if isinstance(df.index, pd.DatetimeIndex):
        return "__index__"
    raise ValueError(f"Could not find a date column. Tried {DATE_CANDIDATE_COLS}")



def _find_ticker_col(df: pd.DataFrame) -> str:
    for col in TICKER_CANDIDATE_COLS:
        if col in df.columns:
            return col
    raise ValueError(f"Could not find a ticker column. Tried {TICKER_CANDIDATE_COLS}")



def _coerce_and_filter_dates(df: pd.DataFrame, start_date: date | None, end_date: date | None) -> pd.DataFrame:
    out = df.copy()
    date_col = _find_date_col(out)
    if date_col == "__index__":
        dates = pd.to_datetime(out.index, errors="coerce")
    else:
        out[date_col] = pd.to_datetime(out[date_col], errors="coerce")
        dates = out[date_col]
    mask = pd.Series(True, index=out.index)
    if start_date is not None:
        mask &= dates >= pd.Timestamp(start_date)
    if end_date is not None:
        mask &= dates <= pd.Timestamp(end_date)
    out = out.loc[mask].copy()
    if date_col != "__index__":
        out = out.sort_values(date_col)
    else:
        out = out.sort_index()
    return out



def _build_frames_from_panel(df: pd.DataFrame, market: str) -> tuple[dict[str, pd.DataFrame], pd.DataFrame, pd.DataFrame | None]:
    ticker_col = _find_ticker_col(df)
    date_col = _find_date_col(df)
    work = df.copy()
    if date_col != "__index__":
        work[date_col] = pd.to_datetime(work[date_col], errors="coerce")
    cfg = get_market_config_v2(market)
    benchmark = cfg["benchmark"]
    universe_frames: dict[str, pd.DataFrame] = {}


    for ticker, g in work.groupby(ticker_col):
        g = g.copy()
        g["ticker"] = str(ticker)
        if date_col != "__index__":
            g = g.sort_values(date_col).set_index(date_col)
        else:
            g = g.sort_index()
        g.index = pd.to_datetime(g.index)
        g.index.name = "date"
        universe_frames[str(ticker)] = g


    bench_df = universe_frames.get(benchmark)

    if bench_df is None:
        for alt in BENCHMARK_FALLBACKS.get(market.upper(), []):
            if alt in universe_frames:
                bench_df = universe_frames[alt]
                logger.warning("Benchmark %s missing; using fallback %s", benchmark, alt)
                break
    if bench_df is None or bench_df.empty:
        raise ValueError(f"Benchmark frame not found for market {market}: expected {benchmark}")
    breadth_df = None
    logger.info("Built market frames: total_symbols=%d benchmark=%s benchmark_rows=%d", len(universe_frames), benchmark, len(bench_df))
    if logger.isEnabledFor(logging.DEBUG):
        lengths = sorted(((k, len(v)) for k, v in universe_frames.items()), key=lambda x: x[1], reverse=True)
        logger.debug("Top symbol frame sizes:\n%s", "\n".join(f"{k}: {n}" for k, n in lengths[:25]))
    return universe_frames, bench_df, breadth_df



def _human_file_size(num_bytes: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(num_bytes)
    for unit in units:
        if size < 1024.0 or unit == units[-1]:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{num_bytes} B"



def load_market_data_v2(
    market: str,
    start_date: date | None = None,
    end_date: date | None = None,
    parquet_path: str | None = None,
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame, pd.DataFrame | None]:
    path = _resolve_parquet_path(market, parquet_path)
    resolved_path = path.expanduser().resolve()
    if not resolved_path.exists():
        raise FileNotFoundError(f"Parquet file not found: {resolved_path}")
    size_bytes = resolved_path.stat().st_size
    logger.info(
        "Resolved parquet file: market=%s override=%s path=%s size_bytes=%d size=%s",
        market.upper(),
        bool(parquet_path),
        resolved_path,
        size_bytes,
        _human_file_size(size_bytes),
    )
    panel = pd.read_parquet(resolved_path)
    logger.info("Rows before date filter: %s", len(panel))
    panel = _coerce_and_filter_dates(panel, start_date, end_date)
    logger.info("Rows after date filter: %s", len(panel))
    if panel.empty:
        raise ValueError("No rows remain after date filtering")
    ticker_col = _find_ticker_col(panel)
    date_col = _find_date_col(panel)
    logger.info("Filtered panel summary: symbols=%d date_col=%s ticker_col=%s", panel[ticker_col].nunique(), date_col, ticker_col)
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("Filtered panel head:\n%s", panel.head(10).to_string(index=False))
        logger.debug("Filtered panel tail:\n%s", panel.tail(10).to_string(index=False))
    return _build_frames_from_panel(panel, market)


# ═══════════════════════════════════════════════════════════════
#  SIGNAL WRITER HELPERS
# ═══════════════════════════════════════════════════════════════

# ── SIGNAL WRITER: canonical action labels ────────────────────
_BUY_LABELS = frozenset({
    "BUY", "STRONG_BUY", "BUY_SCORING", "BUY_ROTATION",
})
_SELL_LABELS = frozenset({
    "SELL", "STRONG_SELL", "SELL_SCORING", "SELL_ROTATION", "REDUCE",
})


def _normalise_action(raw: str) -> str:
    """Map any signal variant to canonical BUY / SELL / HOLD."""
    raw_upper = str(raw).upper().strip()
    if raw_upper in _BUY_LABELS:
        return "BUY"
    if raw_upper in _SELL_LABELS:
        return "SELL"
    return "HOLD"


# ── SIGNAL WRITER: DataFrame column finders ──────────────────

_SCORED_TABLE_KEYS = (
    "final_table", "scored_table", "ranking_table",
    "composite_table", "ticker_scores", "scores",
)
_SCORE_COL_CANDIDATES = (
    "composite_score", "final_score", "score", "total_score",
    "composite", "blended_score",
)
_SIGNAL_COL_CANDIDATES = (
    "signal", "action", "recommendation", "final_signal",
)
_SECTOR_COL_CANDIDATES = (
    "sector", "gics_sector", "industry_sector",
)
_QUADRANT_COL_CANDIDATES = (
    "quadrant", "rotation_quadrant", "regime", "sector_quadrant",
)


def _find_col(df: pd.DataFrame, candidates: tuple[str, ...]) -> str | None:
    """Return the first column from *candidates* that exists in *df*."""
    for col in candidates:
        if col in df.columns:
            return col
    return None


def _resolve_run_date(
    bench_df: pd.DataFrame | None,
    result: dict[str, Any],
) -> str:
    """
    Best-effort run_date extraction.

    Priority: bench_df last index → report header → today.
    """
    # 1. Benchmark last date
    if bench_df is not None and not bench_df.empty:
        try:
            last = bench_df.index[-1]
            if hasattr(last, "strftime"):
                return last.strftime("%Y-%m-%d")
        except Exception:
            pass

    # 2. Report header
    report = result.get("report_v2", {})
    header = report.get("header", {})
    for key in ("as_of_date", "run_date", "date"):
        val = header.get(key)
        if val:
            return str(val)[:10]  # YYYY-MM-DD

    # 3. Fallback
    return date.today().strftime("%Y-%m-%d")


def _extract_action_sets_from_report(
    report: dict[str, Any],
) -> tuple[set[str], set[str]]:
    """
    Pull explicit buy/sell ticker sets out of the report's action
    section, which may be a dict-of-lists, a list-of-dicts, or
    a simple count summary.  Returns (buy_set, sell_set).
    """
    buy_set: set[str] = set()
    sell_set: set[str] = set()
    actions = report.get("actions", {})

    if isinstance(actions, dict):
        # {"buys": [...], "sells": [...]} or {"buy_tickers": [...]}
        for key in ("buys", "buy_tickers", "buy_list"):
            items = actions.get(key, [])
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, dict):
                        t = item.get("ticker") or item.get("symbol")
                        if t:
                            buy_set.add(str(t))
                    elif isinstance(item, str):
                        buy_set.add(item)
        for key in ("sells", "sell_tickers", "sell_list", "reduces", "reduce_tickers"):
            items = actions.get(key, [])
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, dict):
                        t = item.get("ticker") or item.get("symbol")
                        if t:
                            sell_set.add(str(t))
                    elif isinstance(item, str):
                        sell_set.add(item)

    elif isinstance(actions, list):
        # [{"ticker": "AAPL", "action": "BUY"}, ...]
        for item in actions:
            if isinstance(item, dict):
                t = item.get("ticker") or item.get("symbol")
                a = str(item.get("action", "")).upper()
                if t:
                    if a in _BUY_LABELS:
                        buy_set.add(str(t))
                    elif a in _SELL_LABELS:
                        sell_set.add(str(t))

    return buy_set, sell_set


def _extract_portfolio_set(report: dict[str, Any]) -> set[str]:
    """
    Extract the set of tickers selected into the portfolio from the
    report.  These are treated as BUY if no explicit action column
    exists in the scored table.
    """
    selected: set[str] = set()
    portfolio = report.get("portfolio", {})

    # List of holdings / selected names
    for key in ("holdings", "selected", "selected_names", "positions"):
        items = portfolio.get(key, [])
        if isinstance(items, list):
            for item in items:
                if isinstance(item, dict):
                    t = item.get("ticker") or item.get("symbol")
                    if t:
                        selected.add(str(t))
                elif isinstance(item, str):
                    selected.add(item)

    return selected


def _emit_v2_signals(
    market: str,
    result: dict[str, Any],
    bench_df: pd.DataFrame | None = None,
) -> None:
    """
    Extract per-ticker signals from the v2 pipeline result dict
    and write them via signal_writer.

    Tries multiple strategies to locate signal data:
      1. A scored DataFrame in the result (final_table, scored_table, etc.)
      2. Explicit action lists from the report's "actions" section
      3. Portfolio selections as implicit BUY signals

    This makes the integration resilient to pipeline output variations.
    """
    if not _HAS_SIGNAL_WRITER:
        return

    report = result.get("report_v2", {})
    run_date = _resolve_run_date(bench_df, result)

    # ── Strategy 1: scored DataFrame ──────────────────────
    scored_df: pd.DataFrame | None = None
    for key in _SCORED_TABLE_KEYS:
        candidate = result.get(key)
        if isinstance(candidate, pd.DataFrame) and not candidate.empty:
            scored_df = candidate
            logger.debug("Signal writer: using scored table from result['%s'] (%d rows)", key, len(candidate))
            break

    # ── Extract action sets from report (used as override / fallback) ──
    report_buy_set, report_sell_set = _extract_action_sets_from_report(report)
    portfolio_set = _extract_portfolio_set(report)

    # ── Selling exhaustion set (annotate in notes) ────────
    exhaustion_set: set[str] = set()
    exhaustion_scores: dict[str, float] = {}
    exhaustion_table = result.get("selling_exhaustion_table", pd.DataFrame())
    if isinstance(exhaustion_table, pd.DataFrame) and not exhaustion_table.empty:
        ticker_col_ex = _find_col(exhaustion_table, TICKER_CANDIDATE_COLS)
        if ticker_col_ex:
            for _, row in exhaustion_table.iterrows():
                t = str(row[ticker_col_ex])
                exhaustion_set.add(t)
                for scol in ("selling_exhaustion_score", "reversal_trigger_score"):
                    if scol in exhaustion_table.columns and pd.notna(row.get(scol)):
                        exhaustion_scores[t] = round(float(row[scol]), 4)
                        break

    # ── Build signals from scored DataFrame ───────────────
    signals: dict[str, dict] = {}

    if scored_df is not None:
        ticker_col = _find_col(scored_df, TICKER_CANDIDATE_COLS)
        score_col = _find_col(scored_df, _SCORE_COL_CANDIDATES)
        signal_col = _find_col(scored_df, _SIGNAL_COL_CANDIDATES)
        sector_col = _find_col(scored_df, _SECTOR_COL_CANDIDATES)
        quadrant_col = _find_col(scored_df, _QUADRANT_COL_CANDIDATES)

        if ticker_col is None:
            logger.warning("Signal writer: scored table has no ticker column — skipping")
        else:
            buy_rank = 0

            for _, row in scored_df.iterrows():
                ticker = str(row[ticker_col])

                # Score
                score = 0.0
                if score_col and pd.notna(row.get(score_col)):
                    try:
                        score = round(float(row[score_col]), 4)
                    except (TypeError, ValueError):
                        pass

                # Action: prefer explicit column, then report overrides,
                # then portfolio membership
                if signal_col and pd.notna(row.get(signal_col)):
                    raw_signal = str(row[signal_col])
                    action = _normalise_action(raw_signal)
                elif ticker in report_buy_set:
                    raw_signal = "BUY"
                    action = "BUY"
                elif ticker in report_sell_set:
                    raw_signal = "SELL"
                    action = "SELL"
                elif ticker in portfolio_set:
                    raw_signal = "BUY"
                    action = "BUY"
                else:
                    raw_signal = "HOLD"
                    action = "HOLD"

                rank = None
                if action == "BUY":
                    buy_rank += 1
                    rank = buy_rank

                # Sector
                sector = None
                if sector_col and pd.notna(row.get(sector_col)):
                    sector = str(row[sector_col])

                # Regime / quadrant
                regime = None
                if quadrant_col and pd.notna(row.get(quadrant_col)):
                    regime = str(row[quadrant_col])

                # RS rank (try several column names)
                rs_rank = None
                for rs_col in ("rs_rank", "rs_zscore", "relative_strength_rank", "rs_composite"):
                    if rs_col in scored_df.columns and pd.notna(row.get(rs_col)):
                        try:
                            rs_rank = round(float(row[rs_col]), 4)
                        except (TypeError, ValueError):
                            pass
                        break

                # Notes: provenance + exhaustion flag
                notes_parts: list[str] = []
                if raw_signal != action:
                    notes_parts.append(raw_signal)
                if ticker in exhaustion_set:
                    ex_score = exhaustion_scores.get(ticker)
                    if ex_score is not None:
                        notes_parts.append(f"selling_exhaustion={ex_score}")
                    else:
                        notes_parts.append("selling_exhaustion")
                if ticker in report_buy_set and signal_col:
                    notes_parts.append("report_buy")
                if ticker in report_sell_set and signal_col:
                    notes_parts.append("report_sell")

                # ── NEW: tag thematic ETFs in notes ───────
                thematic_map = result.get("thematic_etf_map", {})
                for theme, tickers in thematic_map.items():
                    if ticker in tickers:
                        notes_parts.append(f"theme={theme}")
                        break

                signals[ticker] = {
                    "action":  action,
                    "score":   score,
                    "rank":    rank,
                    "rs_rank": rs_rank,
                    "sector":  sector,
                    "regime":  regime,
                    "notes":   "; ".join(notes_parts) if notes_parts else "",
                }

    # ── Fallback: no scored table, build from report actions + portfolio ──
    if not signals and (report_buy_set or report_sell_set or portfolio_set):
        logger.info("Signal writer: no scored table — building from report actions/portfolio")
        buy_rank = 0
        all_tickers = sorted(report_buy_set | report_sell_set | portfolio_set)

        for ticker in all_tickers:
            if ticker in report_buy_set or ticker in portfolio_set:
                action = "BUY"
                buy_rank += 1
                rank = buy_rank
            elif ticker in report_sell_set:
                action = "SELL"
                rank = None
            else:
                action = "HOLD"
                rank = None

            notes_parts = []
            if ticker in exhaustion_set:
                ex_score = exhaustion_scores.get(ticker)
                if ex_score is not None:
                    notes_parts.append(f"selling_exhaustion={ex_score}")
                else:
                    notes_parts.append("selling_exhaustion")

            signals[ticker] = {
                "action":  action,
                "score":   0.0,
                "rank":    rank,
                "rs_rank": None,
                "sector":  None,
                "regime":  None,
                "notes":   "; ".join(notes_parts) if notes_parts else "",
            }

    if not signals:
        logger.info("Signal writer: no signals to emit for market %s", market)
        return

    # ── Build metadata ────────────────────────────────────
    header = report.get("header", {})
    rotation_section = report.get("rotation", {})
    portfolio_section = report.get("portfolio", {})

    meta: dict[str, Any] = {
        "processed_names":  header.get("processed_names", 0),
        "candidate_count":  portfolio_section.get("candidate_count", 0),
        "selected_count":   portfolio_section.get("selected_count", 0),
    }

    # Rotation quadrants
    if rotation_section.get("available"):
        qc = rotation_section.get("quadrant_counts", {})
        meta["rotation_quadrants"] = {}
        for q in ("leading", "improving", "weakening", "lagging"):
            info = qc.get(q, {})
            meta["rotation_quadrants"][q] = {
                "count":   info.get("count", 0),
                "sectors": info.get("sectors", []),
            }

    # ── NEW: thematic rotation quadrants in metadata ──────
    thematic_rotation = report.get("thematic_rotation", {})
    if thematic_rotation.get("available"):
        tqc = thematic_rotation.get("quadrant_counts", {})
        meta["thematic_rotation_quadrants"] = {}
        for q in ("leading", "improving", "weakening", "lagging"):
            info = tqc.get(q, {})
            meta["thematic_rotation_quadrants"][q] = {
                "count":   info.get("count", 0),
                "themes":  info.get("themes", []),
            }

    # Portfolio rotation exposure
    rot_exp = portfolio_section.get("rotation_exposure", [])
    if rot_exp:
        meta["portfolio_rotation_exposure"] = {
            e.get("quadrant", "?"): round(e.get("weight_pct", 0), 2)
            for e in rot_exp
        }

    # Selling exhaustion summary
    if exhaustion_set:
        meta["selling_exhaustion_count"] = len(exhaustion_set)

    # Skipped names count
    skipped_table = result.get("skipped_table", pd.DataFrame())
    if isinstance(skipped_table, pd.DataFrame) and not skipped_table.empty:
        meta["skipped_count"] = len(skipped_table)

    # ── Write ─────────────────────────────────────────────
    path = _write_signals(
        phase="phase1",
        market=market.upper(),
        run_date=run_date,
        signals=signals,
        model_name="V2 Pipeline (refactor)",
        meta=meta,
    )
    logger.info("V2 signals written → %s  (%d tickers)", path, len(signals))


# ═══════════════════════════════════════════════════════════════
#  STRATEGY RUNNER
# ═══════════════════════════════════════════════════════════════


def run_strategy_v2(
    market: str,
    universe_frames: dict[str, pd.DataFrame],
    bench_df: pd.DataFrame,
    breadth_df: pd.DataFrame | None = None,
    config: dict | None = None,
) -> dict[str, Any]:
    cfg = get_market_config_v2(market)
    tradable = set(cfg["tradable_universe"])
    leadership = set(cfg["leadership_universe"])
    tradable_frames = {k: v for k, v in universe_frames.items() if k in tradable}
    leadership_frames = {k: v for k, v in universe_frames.items() if k in leadership}
    logger.info(
        "Universe selection: configured_tradable=%d configured_leadership=%d matched_tradable=%d matched_leadership=%d",
        len(tradable), len(leadership), len(tradable_frames), len(leadership_frames),
    )
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("Matched tradable tickers (first 100): %s", sorted(tradable_frames.keys())[:100])
        logger.debug("Matched leadership tickers (first 100): %s", sorted(leadership_frames.keys())[:100])
    missing_tradable = sorted(tradable - set(tradable_frames.keys()))
    missing_leadership = sorted(leadership - set(leadership_frames.keys()))
    if missing_tradable:
        logger.info("Missing tradable symbols in parquet: count=%d sample=%s", len(missing_tradable), missing_tradable[:25])
    if missing_leadership:
        logger.info("Missing leadership symbols in parquet: count=%d sample=%s", len(missing_leadership), missing_leadership[:25])

    # ── NEW: extract thematic ETF frames ──────────────────────────────────
    thematic_frames, available_thematic_map = _extract_thematic_frames(
        universe_frames, market,
    )

    # ── Build portfolio params from market config ─────────────────────────
    portfolio_params = {
        "max_positions": cfg.get("max_positions", 8),
        "max_sector_weight": cfg.get("max_sector_weight", 0.35),
        "max_theme_names": cfg.get("max_theme_names", 2),
        "max_single_weight": cfg.get("max_single_weight", 0.20),
        "min_weight": cfg.get("min_weight", 0.04),
    }

    # ── CHANGED: inject thematic data into config for pipeline ────────────
    effective_config = dict(config or {})
    effective_config["thematic_etf_frames"] = thematic_frames
    effective_config["thematic_etf_map"] = available_thematic_map

    result = run_pipeline_v2(
        tradable_frames=tradable_frames,
        leadership_frames=leadership_frames,
        bench_df=bench_df,
        breadth_df=breadth_df,
        market=cfg["market"],
        portfolio_params=portfolio_params,
        config=effective_config,               # ← thematic data rides here
    )
    result["market_config_v2"] = cfg
    result["tradable_universe"] = sorted(tradable)
    result["leadership_universe"] = sorted(leadership)

    # ── NEW: persist thematic metadata in result for report / signals ─────
    result["thematic_etf_frames"] = thematic_frames
    result["thematic_etf_map"] = available_thematic_map

    # ── Log skipped names ─────────────────────────────────────────────────
    skipped_table = result.get("skipped_table", pd.DataFrame())
    if isinstance(skipped_table, pd.DataFrame) and not skipped_table.empty:
        sample_cols = [c for c in ["ticker", "missing_critical_fields_v2"] if c in skipped_table.columns]
        logger.info(
            "Skipped symbols due to scoreability gate: count=%d sample=%s",
            len(skipped_table),
            skipped_table[sample_cols].head(20).to_dict(orient="records") if sample_cols else skipped_table.head(20).to_dict(orient="records"),
        )

    # ── Log selling exhaustion ────────────────────────────────────────────
    exhaustion_table = result.get("selling_exhaustion_table", pd.DataFrame())
    if isinstance(exhaustion_table, pd.DataFrame) and not exhaustion_table.empty:
        logger.info(
            "Selling exhaustion candidates: count=%d",
            len(exhaustion_table),
        )
        # Status breakdown
        if "status" in exhaustion_table.columns:
            status_counts = exhaustion_table["status"].value_counts(dropna=False).to_dict()
            logger.info(
                "Selling exhaustion status breakdown: %s", status_counts,
            )
        # Quality breakdown
        if "quality_label" in exhaustion_table.columns:
            quality_counts = exhaustion_table["quality_label"].value_counts(dropna=False).to_dict()
            logger.info(
                "Selling exhaustion quality breakdown: %s", quality_counts,
            )
        # Top candidates preview
        preview_cols = [
            c for c in [
                "ticker", "status", "quality_label",
                "selling_exhaustion_score", "reversal_trigger_score",
                "rsi_14", "price_5d_change", "sector",
            ] if c in exhaustion_table.columns
        ]
        if preview_cols:
            logger.info(
                "Selling exhaustion top candidates:\n%s",
                exhaustion_table[preview_cols].head(10).to_string(index=False),
            )

        # Sector distribution of exhaustion candidates
        if "sector" in exhaustion_table.columns:
            sector_counts = exhaustion_table["sector"].value_counts(dropna=False).to_dict()
            logger.info(
                "Selling exhaustion by sector: %s", sector_counts,
            )
    else:
        logger.info("Selling exhaustion: no candidates detected")

    # ── Build report ──────────────────────────────────────────────────────
    report = build_report_v2(result)
    result["report_v2"] = report
    result["report_text_v2"] = to_text_v2(report)

    # ── NEW: portfolio selection gap diagnostics (answers Q1) ─────────────
    _log_portfolio_selection_gaps(result)

    # ── SIGNAL WRITER: emit signals after report is built ─────────────────
    _emit_v2_signals(market, result, bench_df)

    # ── Summary logging (read from nested report structure) ───────────────
    header = report.get("header", {})
    portfolio_section = report.get("portfolio", {})
    action_summary = report.get("actions", {})
    rotation_section = report.get("rotation", {})
    exhaustion_section = report.get("selling_exhaustion", {})

    logger.info(
        "Strategy result summary: "
        "processed=%s candidates=%s selected=%s "
        "actions=%s exhaustion=%s",
        header.get("processed_names", 0),
        portfolio_section.get("candidate_count", 0),
        portfolio_section.get("selected_count", 0),
        action_summary,
        exhaustion_section.get("count", 0),
    )

    # ── Rotation summary ──────────────────────────────────────────────────
    if rotation_section.get("available"):
        qc = rotation_section.get("quadrant_counts", {})
        parts = []
        for q in ("leading", "improving", "weakening", "lagging"):
            info = qc.get(q, {})
            count = info.get("count", 0)
            sectors = info.get("sectors", [])
            parts.append(f"{q}={count}({','.join(sectors[:3])})")
        logger.info("Sector rotation: %s", "  ".join(parts))

    # ── NEW: thematic rotation summary ────────────────────────────────────
    thematic_rotation = report.get("thematic_rotation", {})
    if thematic_rotation.get("available"):
        tqc = thematic_rotation.get("quadrant_counts", {})
        parts = []
        for q in ("leading", "improving", "weakening", "lagging"):
            info = tqc.get(q, {})
            count = info.get("count", 0)
            themes = info.get("themes", info.get("sectors", []))
            parts.append(f"{q}={count}({','.join(themes[:4])})")
        logger.info("Thematic rotation: %s", "  ".join(parts))
    elif available_thematic_map:
        logger.info(
            "Thematic rotation: not yet computed by pipeline "
            "(available themes: %s — update pipeline_v2 to consume "
            "config['thematic_etf_frames'])",
            list(available_thematic_map.keys()),
        )

    # ── Portfolio rotation exposure ───────────────────────────────────────
    rot_exp = portfolio_section.get("rotation_exposure", [])
    if rot_exp:
        exp_parts = [
            f"{e.get('quadrant', '?')}={e.get('weight_pct', 0):.1f}%"
            for e in rot_exp
        ]
        logger.info("Portfolio rotation exposure: %s", "  ".join(exp_parts))

    return result



def main(argv=None):
    args = build_parser().parse_args(argv)
    log_file = setup_logging(args.verbose)
    try:
        logger.info("Starting runner_v2")
        logger.info("Log file: %s", log_file)
        logger.info("Market: %s", args.market)
        logger.info("Start date: %s", args.start_date)
        logger.info("End date: %s", args.end_date)
        logger.info("Verbose logging: %s", args.verbose)

        # ── SIGNAL WRITER: log availability at startup ────────
        if _HAS_SIGNAL_WRITER:
            logger.info("Signal writer: enabled → results/signals/")
        else:
            logger.info("Signal writer: not available (install signal_writer.py for combined reports)")

        universe_frames, bench_df, breadth_df = load_market_data_v2(
            market=args.market,
            start_date=args.start_date,
            end_date=args.end_date,
            parquet_path=args.parquet_path,
        )

        result = run_strategy_v2(
            market=args.market,
            universe_frames=universe_frames,
            bench_df=bench_df,
            breadth_df=breadth_df,
        )

        run_log = RunLogger(f"runner_v2_{args.market}")
        print_run_summary(result, args.market, run_log)
        # ─────────────────────────────────────────────────────

        logger.info("runner_v2 completed")

        if args.print_report and result.get("report_text_v2"):
            print(result["report_text_v2"])

        return result

    except Exception as exc:
        logger.exception("runner_v2 failed: %s", exc)
        raise



if __name__ == "__main__":
    main()