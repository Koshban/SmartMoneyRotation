"""refactor/runner_v2.py"""
from __future__ import annotations
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
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

# ── HTML REPORT: optional import ──────────────────────────────
try:
    from utils.html_report import build_html_report, save_html_report
    _HAS_HTML_REPORT = True
except ImportError:
    _HAS_HTML_REPORT = False

# ── EMAIL: optional import ────────────────────────────────────
try:
    from utils.email_report import send_report_email
    _HAS_EMAIL = True
except ImportError:
    _HAS_EMAIL = False


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
#  THEMATIC ETF DEFINITIONS
# ═══════════════════════════════════════════════════════════════

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

THEMATIC_ETF_TICKERS: dict[str, set[str]] = {
    mkt: {
        etf
        for etfs in themes.values()
        for etf in etfs
    }
    for mkt, themes in THEMATIC_ETF_MAP.items()
}

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
#  DISPLAY FORMATTING HELPERS
# ═══════════════════════════════════════════════════════════════

_BOX_WIDTH = 110

_REGIME_ICON: dict[str, str] = {
    "leading":   "🟢",
    "improving": "🔵",
    "weakening": "🟡",
    "lagging":   "🔴",
    "unknown":   "⚪",
}


def _box_header(emoji: str, title: str) -> list[str]:
    """Produce a ╔═══╗ / ║ ... ║ / ╚═══╝ header block."""
    inner = _BOX_WIDTH - 2
    content = f" {emoji}  {title} "
    padded = content + " " * max(0, inner - len(content))
    return [
        "╔" + "═" * inner + "╗",
        "║" + padded[:inner] + "║",
        "╚" + "═" * inner + "╝",
    ]


def _sub_header(text: str, width: int = 90) -> str:
    """Produce a ── Title (extra) ── style sub-header line."""
    dash_len = max(4, width - len(text) - 6)
    return f"  ── {text}  {'─' * dash_len}"


def _hbar(value: float, max_val: float, width: int = 16) -> str:
    """Render a thin Unicode horizontal bar chart segment."""
    if max_val <= 0:
        return " " * width
    ratio = max(0.0, min(1.0, value / max_val))
    filled = ratio * width
    full = int(filled)
    frac = filled - full
    partials = " ▏▎▍▌▋▊▉"
    bar = "█" * full
    if full < width:
        bar += partials[min(int(frac * 8), 7)]
        bar += " " * (width - full - 1)
    return bar[:width]


def _signed(val: float, width: int = 8, decimals: int = 4, pct: bool = False) -> str:
    """Format a signed numeric value with explicit +/- prefix."""
    if pct:
        formatted = f"{val * 100:+.2f}%"
    else:
        formatted = f"{val:+.{decimals}f}"
    return formatted.rjust(width)


def _detect_stale_columns(
    df: pd.DataFrame,
    columns: list[str],
    threshold: float = 0.95,
) -> list[str]:
    """Return column names where ≥ threshold fraction of non-null values are identical."""
    stale: list[str] = []
    for col in columns:
        if col not in df.columns:
            continue
        series = df[col].dropna()
        if len(series) < 3:
            continue
        if series.nunique() <= 1:
            stale.append(col)
            continue
        top_pct = series.value_counts(normalize=True).iloc[0]
        if top_pct >= threshold:
            stale.append(col)
    return stale


# ═══════════════════════════════════════════════════════════════
#  THEMATIC FRAME EXTRACTION
# ═══════════════════════════════════════════════════════════════

def _extract_thematic_frames(
    universe_frames: dict[str, pd.DataFrame],
    market: str,
) -> tuple[dict[str, pd.DataFrame], dict[str, list[str]]]:
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
#  PORTFOLIO SELECTION GAP DIAGNOSTICS
# ═══════════════════════════════════════════════════════════════

def _log_portfolio_selection_gaps(result: dict[str, Any]) -> None:
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

    sb_mask = scored_df[signal_col].astype(str).str.upper() == "STRONG_BUY"
    all_strong_buy = set(scored_df.loc[sb_mask, ticker_col].astype(str))
    if not all_strong_buy:
        return

    report = result.get("report_v2", {})
    selected_set = _extract_portfolio_set(report)

    for key in ("portfolio_tickers", "selected_tickers", "selected_names"):
        extra = result.get(key)
        if isinstance(extra, (list, set, frozenset)):
            selected_set |= {str(t) for t in extra}

    dropped = sorted(all_strong_buy - selected_set)
    if dropped:
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
    p.add_argument("--no-email", action="store_true", help="Skip emailing the HTML report")
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

_BUY_LABELS = frozenset({
    "BUY", "STRONG_BUY", "BUY_SCORING", "BUY_ROTATION",
})
_SELL_LABELS = frozenset({
    "SELL", "STRONG_SELL", "SELL_SCORING", "SELL_ROTATION", "REDUCE",
})


def _normalise_action(raw: str) -> str:
    raw_upper = str(raw).upper().strip()
    if raw_upper in _BUY_LABELS:
        return "BUY"
    if raw_upper in _SELL_LABELS:
        return "SELL"
    return "HOLD"


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
    for col in candidates:
        if col in df.columns:
            return col
    return None


def _resolve_run_date(
    bench_df: pd.DataFrame | None,
    result: dict[str, Any],
) -> str:
    if bench_df is not None and not bench_df.empty:
        try:
            last = bench_df.index[-1]
            if hasattr(last, "strftime"):
                return last.strftime("%Y-%m-%d")
        except Exception:
            pass

    report = result.get("report_v2", {})
    header = report.get("header", {})
    for key in ("as_of_date", "run_date", "date"):
        val = header.get(key)
        if val:
            return str(val)[:10]

    return date.today().strftime("%Y-%m-%d")


def _extract_action_sets_from_report(
    report: dict[str, Any],
) -> tuple[set[str], set[str]]:
    buy_set: set[str] = set()
    sell_set: set[str] = set()
    actions = report.get("actions", {})

    if isinstance(actions, dict):
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
    selected: set[str] = set()
    portfolio = report.get("portfolio", {})

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
    if not _HAS_SIGNAL_WRITER:
        return

    report = result.get("report_v2", {})
    run_date = _resolve_run_date(bench_df, result)

    scored_df: pd.DataFrame | None = None
    for key in _SCORED_TABLE_KEYS:
        candidate = result.get(key)
        if isinstance(candidate, pd.DataFrame) and not candidate.empty:
            scored_df = candidate
            logger.debug("Signal writer: using scored table from result['%s'] (%d rows)", key, len(candidate))
            break

    report_buy_set, report_sell_set = _extract_action_sets_from_report(report)
    portfolio_set = _extract_portfolio_set(report)

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

                score = 0.0
                if score_col and pd.notna(row.get(score_col)):
                    try:
                        score = round(float(row[score_col]), 4)
                    except (TypeError, ValueError):
                        pass

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

                sector = None
                if sector_col and pd.notna(row.get(sector_col)):
                    sector = str(row[sector_col])

                regime = None
                if quadrant_col and pd.notna(row.get(quadrant_col)):
                    regime = str(row[quadrant_col])

                rs_rank = None
                for rs_col in ("rs_rank", "rs_zscore", "relative_strength_rank", "rs_composite"):
                    if rs_col in scored_df.columns and pd.notna(row.get(rs_col)):
                        try:
                            rs_rank = round(float(row[rs_col]), 4)
                        except (TypeError, ValueError):
                            pass
                        break

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

    header = report.get("header", {})
    rotation_section = report.get("rotation", {})
    portfolio_section = report.get("portfolio", {})

    meta: dict[str, Any] = {
        "processed_names":  header.get("processed_names", 0),
        "candidate_count":  portfolio_section.get("candidate_count", 0),
        "selected_count":   portfolio_section.get("selected_count", 0),
    }

    if rotation_section.get("available"):
        qc = rotation_section.get("quadrant_counts", {})
        meta["rotation_quadrants"] = {}
        for q in ("leading", "improving", "weakening", "lagging"):
            info = qc.get(q, {})
            meta["rotation_quadrants"][q] = {
                "count":   info.get("count", 0),
                "sectors": info.get("sectors", []),
            }

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

    rot_exp = portfolio_section.get("rotation_exposure", [])
    if rot_exp:
        meta["portfolio_rotation_exposure"] = {
            e.get("quadrant", "?"): round(e.get("weight_pct", 0), 2)
            for e in rot_exp
        }

    if exhaustion_set:
        meta["selling_exhaustion_count"] = len(exhaustion_set)

    skipped_table = result.get("skipped_table", pd.DataFrame())
    if isinstance(skipped_table, pd.DataFrame) and not skipped_table.empty:
        meta["skipped_count"] = len(skipped_table)

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
#  SECTOR ROTATION DISPLAY (Blended RRG + ETF Composite)
# ═══════════════════════════════════════════════════════════════

def _display_sector_rotation_v2(result: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    sector_summary: pd.DataFrame = result.get("sector_summary", pd.DataFrame())

    if isinstance(sector_summary, pd.DataFrame) and not sector_summary.empty:
        lines.append("")
        lines.extend(_box_header("📊", "SECTOR ROTATION — Blended RRG + ETF Composite"))
        lines.append("")

        regime_groups: dict[str, list[str]] = {}
        for _, row in sector_summary.iterrows():
            r = str(row.get("regime", "unknown"))
            regime_groups.setdefault(r, []).append(str(row.get("sector", "?")))

        for regime_name in ("leading", "improving", "weakening", "lagging"):
            members = regime_groups.get(regime_name, [])
            icon = _REGIME_ICON.get(regime_name, "⚪")
            label = regime_name.upper()
            lines.append(
                f"  {icon} {label:<14}({len(members):>2})  "
                + (", ".join(members) if members else "—")
            )

        lines.append("")

        has_blended    = "blended_score"   in sector_summary.columns
        has_rs_level   = "rs_level"        in sector_summary.columns
        has_rs_mom     = "rs_mom"          in sector_summary.columns
        has_etf_comp   = "etf_composite"   in sector_summary.columns
        has_theme_avg  = "theme_avg_score" in sector_summary.columns
        has_excess     = "excess_20d"      in sector_summary.columns
        has_rrg_quad   = "rrg_quadrant"    in sector_summary.columns

        hdr = f"  {'#':>4}  {'Sector':<25} {'ETF':<6}  {'Regime':<16}"
        if has_blended:
            hdr += f"  {'Blended':>22}"
        if has_rs_level:
            hdr += f"  {'RS Lvl':>9}"
        if has_rs_mom:
            hdr += f"  {'RS Mom':>9}"
        if has_etf_comp:
            hdr += f"  {'ETF Scr':>8}"
        if has_theme_avg:
            hdr += f"  {'Thm Avg':>8}"
        if has_excess:
            hdr += f"  {'Excess20d':>10}"
        if has_rrg_quad:
            hdr += f"  {'RRG Quad':>16}"

        lines.append(hdr)
        lines.append("  " + "━" * (len(hdr)))

        max_blended = 0.80
        if has_blended and not sector_summary["blended_score"].empty:
            max_blended = max(0.80, sector_summary["blended_score"].max() * 1.15)

        for i, (_, row) in enumerate(sector_summary.iterrows(), 1):
            sector_name = str(row.get("sector", "?"))
            etf_ticker  = str(row.get("etf", "?"))
            regime      = str(row.get("regime", "?"))
            icon        = _REGIME_ICON.get(regime, "⚪")

            rl = f"  {i:>4}  {sector_name:<25} {etf_ticker:<6}  {icon} {regime:<12}"

            if has_blended:
                bv = float(row.get("blended_score", 0))
                bar = _hbar(bv, max_blended, width=12)
                rl += f"  {bar} {bv:.4f}"
            if has_rs_level:
                rl += f"  {_signed(float(row.get('rs_level', 0)), width=9)}"
            if has_rs_mom:
                rl += f"  {_signed(float(row.get('rs_mom', 0)), width=9)}"
            if has_etf_comp:
                rl += f"  {float(row.get('etf_composite', 0)):>8.4f}"
            if has_theme_avg:
                rl += f"  {float(row.get('theme_avg_score', 0)):>8.4f}"
            if has_excess:
                rl += f"  {_signed(float(row.get('excess_20d', 0)), width=10, pct=True)}"
            if has_rrg_quad:
                rq = str(row.get("rrg_quadrant", ""))
                rq_icon = _REGIME_ICON.get(rq, "⚪")
                rl += f"  {rq_icon} {rq:<12}"

            lines.append(rl)

        lines.append("")
        return lines

    report = result.get("report_v2", {})
    rotation_section = report.get("rotation", {})
    if not rotation_section.get("available"):
        lines.append("")
        lines.extend(_box_header("📊", "SECTOR ROTATION"))
        lines.append("  (not available)")
        return lines

    lines.append("")
    lines.extend(_box_header("📊", "SECTOR ROTATION — RRG Quadrants (Legacy)"))
    lines.append("")

    qc = rotation_section.get("quadrant_counts", {})
    for regime_name in ("leading", "improving", "weakening", "lagging"):
        info = qc.get(regime_name, {})
        count = info.get("count", 0)
        sectors = info.get("sectors", [])
        icon = _REGIME_ICON.get(regime_name, "⚪")
        label = regime_name.upper()
        lines.append(
            f"  {icon} {label:<14}({count:>2})  "
            + (", ".join(sectors) if sectors else "—")
        )

    sector_detail = rotation_section.get("sector_detail", [])
    if sector_detail:
        lines.append("")
        hdr = (
            f"  {'#':>4}  {'Sector':<25} {'ETF':<6}  {'Regime':<16}"
            f"  {'RS Level':>10}  {'RS Mom':>8}  {'Excess 20d':>10}"
        )
        lines.append(hdr)
        lines.append("  " + "━" * len(hdr))
        for i, sd in enumerate(sector_detail, 1):
            regime = sd.get("regime", "?")
            icon = _REGIME_ICON.get(regime, "⚪")
            lines.append(
                f"  {i:>4}  {sd.get('sector', '?'):<25} "
                f"{sd.get('etf', '?'):<6}  {icon} {regime:<12}"
                f"  {sd.get('rs_level', 0):>10.4f}"
                f"  {sd.get('rs_mom', 0):>8.4f}"
                f"  {_signed(sd.get('excess_20d', 0), width=10, pct=True)}"
            )

    lines.append("")
    return lines


# ═══════════════════════════════════════════════════════════════
#  ETF UNIVERSE RANKING DISPLAY
# ═══════════════════════════════════════════════════════════════

def _display_etf_ranking(result: dict[str, Any], max_rows: int = 40) -> list[str]:
    lines: list[str] = []
    etf_ranking: pd.DataFrame = result.get("etf_ranking", pd.DataFrame())

    if not isinstance(etf_ranking, pd.DataFrame) or etf_ranking.empty:
        lines.append("")
        lines.extend(_box_header("📈", "ETF UNIVERSE RANKING"))
        lines.append("  (not available — rotation_v2 ETF scoring not yet active)")
        lines.append("")
        return lines

    lines.append("")
    lines.extend(_box_header("📈", "ETF UNIVERSE RANKING — by Composite Score"))
    lines.append("")

    comp_col = "etf_composite"
    n_total = len(etf_ranking)
    mean_sc = etf_ranking[comp_col].mean() if comp_col in etf_ranking.columns else 0.0
    top_ticker = str(etf_ranking.iloc[0].get("ticker", "?")) if n_total > 0 else "?"
    top_score = float(etf_ranking.iloc[0].get(comp_col, 0)) if n_total > 0 else 0.0
    bot_ticker = str(etf_ranking.iloc[-1].get("ticker", "?")) if n_total > 0 else "?"
    bot_score = float(etf_ranking.iloc[-1].get(comp_col, 0)) if n_total > 0 else 0.0

    lines.append(
        f"  ETFs scored: {n_total}     "
        f"Mean: {mean_sc:.3f}     "
        f"Top: {top_ticker} ({top_score:.3f})     "
        f"Bottom: {bot_ticker} ({bot_score:.3f})"
    )
    lines.append("")

    _CHECK_COLS: dict[str, str] = {
        "sub_trend":          "Trend",
        "sub_participation":  "Part",
        "rsi14":              "RSI",
        "adx14":              "ADX",
        "relativevolume":     "RVOL",
    }
    stale_internal = _detect_stale_columns(etf_ranking, list(_CHECK_COLS.keys()))
    stale_labels: set[str] = {_CHECK_COLS[c] for c in stale_internal}

    if stale_internal:
        stale_vals_parts: list[str] = []
        for col in stale_internal:
            if col in etf_ranking.columns:
                mode_series = etf_ranking[col].mode()
                mode_val = mode_series.iloc[0] if not mode_series.empty else "?"
                if isinstance(mode_val, float):
                    mode_val = f"{mode_val:.3f}" if mode_val < 10 else f"{mode_val:.1f}"
                stale_vals_parts.append(f"{_CHECK_COLS[col]}={mode_val}")

        lines.append("  ⚠️  DATA QUALITY WARNING")
        lines.append(
            f"  │  Columns marked † show identical values across all ETFs "
            f"({', '.join(stale_vals_parts)})."
        )
        lines.append(
            "  │  Composite score is effectively driven by the remaining "
            "varying columns only."
        )
        lines.append(
            "  │  Check the upstream scoring pipeline for these indicators."
        )
        lines.append("  └" + "─" * 95)
        lines.append("")

    has_theme       = "theme"              in etf_ranking.columns
    has_sector      = "parent_sector"      in etf_ranking.columns
    has_composite   = comp_col             in etf_ranking.columns
    has_trend       = "sub_trend"          in etf_ranking.columns
    has_momentum    = "sub_momentum"       in etf_ranking.columns
    has_part        = "sub_participation"  in etf_ranking.columns
    has_rsi         = "rsi14"              in etf_ranking.columns
    has_adx         = "adx14"              in etf_ranking.columns
    has_rvol        = "relativevolume"     in etf_ranking.columns
    has_ret20       = "return20d"          in etf_ranking.columns
    has_is_sector   = "is_sector_etf"      in etf_ranking.columns
    has_is_broad    = "is_broad"           in etf_ranking.columns
    has_is_regional = "is_regional"        in etf_ranking.columns

    def _clbl(display_name: str, internal_col: str) -> str:
        return display_name + "†" if internal_col in stale_internal else display_name

    hdr = f"  {'#':>4}  {'Ticker':<9}"
    if has_theme:
        hdr += f" {'Theme':<22}"
    if has_sector:
        hdr += f" {'Sector':<20}"
    if has_composite:
        hdr += f"  {'Score':>18}"
    if has_momentum:
        hdr += f"  {_clbl('Mom', 'sub_momentum'):>6}"
    if has_trend:
        hdr += f"  {_clbl('Trend', 'sub_trend'):>7}"
    if has_part:
        hdr += f"  {_clbl('Part', 'sub_participation'):>6}"
    if has_rsi:
        hdr += f"  {_clbl('RSI', 'rsi14'):>6}"
    if has_adx:
        hdr += f"  {_clbl('ADX', 'adx14'):>6}"
    if has_rvol:
        hdr += f"  {_clbl('RVOL', 'relativevolume'):>6}"
    if has_ret20:
        hdr += f"  {'Ret20d':>8}"

    lines.append(hdr)
    lines.append("  " + "━" * len(hdr))

    display_df = etf_ranking.copy()
    if has_is_regional:
        display_df = display_df[~display_df["is_regional"]].copy()
    display_df = display_df.head(max_rows)

    max_score = 0.60
    if has_composite and not display_df.empty:
        max_score = max(0.60, display_df[comp_col].max() * 1.10)

    for i, (_, row) in enumerate(display_df.iterrows(), 1):
        ticker = str(row.get("ticker", "?"))

        marker = " "
        if has_is_sector and row.get("is_sector_etf"):
            marker = "●"
        elif has_is_broad and row.get("is_broad"):
            marker = "○"

        rl = f"  {i:>4}  {ticker:<6}{marker} "

        if has_theme:
            theme = str(row.get("theme", ""))[:21]
            rl += f" {theme:<22}"
        if has_sector:
            sector = str(row.get("parent_sector", ""))[:19]
            rl += f" {sector:<20}"
        if has_composite:
            sv = float(row.get(comp_col, 0))
            bar = _hbar(sv, max_score, width=10)
            rl += f"  {bar} {sv:.3f}"
        if has_momentum:
            rl += f"  {float(row.get('sub_momentum', 0)):>6.3f}"
        if has_trend:
            rl += f"  {float(row.get('sub_trend', 0)):>7.3f}"
        if has_part:
            rl += f"  {float(row.get('sub_participation', 0)):>6.3f}"
        if has_rsi:
            rl += f"  {float(row.get('rsi14', 50)):>6.1f}"
        if has_adx:
            rl += f"  {float(row.get('adx14', 15)):>6.1f}"
        if has_rvol:
            rl += f"  {float(row.get('relativevolume', 1)):>6.2f}"
        if has_ret20:
            ret = float(row.get("return20d", 0))
            rl += f"  {ret:>+8.1%}"

        lines.append(rl)

    lines.append("")
    legend_parts = ["  (● = sector ETF   ○ = broad market)"]
    if stale_labels:
        legend_parts.append(
            f"  († = stale data — identical across all ETFs: "
            f"{', '.join(sorted(stale_labels))})"
        )
    lines.extend(legend_parts)
    lines.append("")

    return lines


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

    thematic_frames, available_thematic_map = _extract_thematic_frames(
        universe_frames, market,
    )

    portfolio_params = {
        "max_positions": cfg.get("max_positions", 8),
        "max_sector_weight": cfg.get("max_sector_weight", 0.35),
        "max_theme_names": cfg.get("max_theme_names", 2),
        "max_single_weight": cfg.get("max_single_weight", 0.20),
        "min_weight": cfg.get("min_weight", 0.04),
    }

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
        config=effective_config,
    )
    result["market_config_v2"] = cfg
    result["tradable_universe"] = sorted(tradable)
    result["leadership_universe"] = sorted(leadership)

    result["thematic_etf_frames"] = thematic_frames
    result["thematic_etf_map"] = available_thematic_map

    skipped_table = result.get("skipped_table", pd.DataFrame())
    if isinstance(skipped_table, pd.DataFrame) and not skipped_table.empty:
        sample_cols = [c for c in ["ticker", "missing_critical_fields_v2"] if c in skipped_table.columns]
        logger.info(
            "Skipped symbols due to scoreability gate: count=%d sample=%s",
            len(skipped_table),
            skipped_table[sample_cols].head(20).to_dict(orient="records") if sample_cols else skipped_table.head(20).to_dict(orient="records"),
        )

    exhaustion_table = result.get("selling_exhaustion_table", pd.DataFrame())
    if isinstance(exhaustion_table, pd.DataFrame) and not exhaustion_table.empty:
        logger.info(
            "Selling exhaustion candidates: count=%d",
            len(exhaustion_table),
        )
        if "status" in exhaustion_table.columns:
            status_counts = exhaustion_table["status"].value_counts(dropna=False).to_dict()
            logger.info(
                "Selling exhaustion status breakdown: %s", status_counts,
            )
        if "quality_label" in exhaustion_table.columns:
            quality_counts = exhaustion_table["quality_label"].value_counts(dropna=False).to_dict()
            logger.info(
                "Selling exhaustion quality breakdown: %s", quality_counts,
            )
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

        if "sector" in exhaustion_table.columns:
            sector_counts = exhaustion_table["sector"].value_counts(dropna=False).to_dict()
            logger.info(
                "Selling exhaustion by sector: %s", sector_counts,
            )
    else:
        logger.info("Selling exhaustion: no candidates detected")

    report = build_report_v2(result)
    result["report_v2"] = report
    result["report_text_v2"] = to_text_v2(report)

    _log_portfolio_selection_gaps(result)

    _emit_v2_signals(market, result, bench_df)

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

    if rotation_section.get("available"):
        qc = rotation_section.get("quadrant_counts", {})
        parts = []
        for q in ("leading", "improving", "weakening", "lagging"):
            info = qc.get(q, {})
            count = info.get("count", 0)
            sectors = info.get("sectors", [])
            parts.append(f"{q}={count}({','.join(sectors[:3])})")
        logger.info("Sector rotation: %s", "  ".join(parts))

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

    rot_exp = portfolio_section.get("rotation_exposure", [])
    if rot_exp:
        exp_parts = [
            f"{e.get('quadrant', '?')}={e.get('weight_pct', 0):.1f}%"
            for e in rot_exp
        ]
        logger.info("Portfolio rotation exposure: %s", "  ".join(exp_parts))

    rotation_lines = _display_sector_rotation_v2(result)
    etf_lines = _display_etf_ranking(result, max_rows=40)

    all_display_lines = rotation_lines + etf_lines
    if all_display_lines:
        display_block = "\n".join(all_display_lines)
        logger.info("Sector Rotation & ETF Ranking:\n%s", display_block)
        result["sector_rotation_display"] = display_block

    return result


# ═══════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════

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

        if _HAS_SIGNAL_WRITER:
            logger.info("Signal writer: enabled → results/signals/")
        else:
            logger.info("Signal writer: not available (install signal_writer.py for combined reports)")

        if _HAS_HTML_REPORT:
            logger.info("HTML report: enabled")
        else:
            logger.info("HTML report: not available (install utils/html_report.py)")

        if _HAS_EMAIL and not args.no_email:
            logger.info("Email report: enabled")
        elif args.no_email:
            logger.info("Email report: disabled via --no-email")
        else:
            logger.info("Email report: not available (install utils/email_report.py)")

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

        # ── Console summary (existing) ────────────────────────────────
        run_log = RunLogger(f"runner_v2_{args.market}")
        print_run_summary(result, args.market, run_log)

        # ── HTML Report: build, save, print link ──────────────────────
        html_path = None
        html_content = None
        if _HAS_HTML_REPORT:
            try:
                html_content = build_html_report(result, args.market)
                html_path = save_html_report(html_content, args.market)
                logger.info("HTML report: %s", html_path)

                # Print a clickable file-URL
                try:
                    file_url = html_path.as_uri()
                except Exception:
                    file_url = f"file:///{html_path.as_posix()}"

                print(f"\n{'─' * 72}")
                print(f"📊  HTML Report saved → {html_path}")
                print(f"    Open in browser:    {file_url}")
            except Exception as exc:
                logger.error("HTML report generation failed: %s", exc)
        else:
            print(f"\n{'─' * 72}")
            print("📊  HTML report: skipped (utils/html_report.py not found)")

        # ── Email Report ──────────────────────────────────────────────
        if _HAS_EMAIL and not args.no_email and html_content:
            try:
                run_date = _resolve_run_date(bench_df, result)
                subject = (
                    f"Smart Money Rotation — {args.market.upper()} — {run_date}"
                )
                ok = send_report_email(
                    html_content=html_content,
                    subject=subject,
                    html_path=html_path,
                )
                if ok:
                    print(f"📧  Report emailed successfully")
                else:
                    print(f"📧  Email not sent (check common/credential.py)")
            except Exception as exc:
                logger.error("Email sending failed: %s", exc)
                print(f"📧  Email failed: {exc}")
        elif args.no_email:
            print(f"📧  Email skipped (--no-email)")
        else:
            print(f"📧  Email not available (install utils/email_report.py + common/credential.py)")

        print(f"{'─' * 72}\n")

        logger.info("runner_v2 completed")

        if args.print_report and result.get("report_text_v2"):
            print(result["report_text_v2"])

        return result

    except Exception as exc:
        logger.exception("runner_v2 failed: %s", exc)
        raise


if __name__ == "__main__":
    main()