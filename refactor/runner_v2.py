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
from .common.market_config_v2 import get_market_config_v2
from .pipeline_v2 import run_pipeline_v2
from .report_v2 import build_report_v2, to_text_v2


logger = logging.getLogger("refactor.runner_v2")


MARKET_PARQUET = {
    "US": "us_cash.parquet",
    "HK": "hk_cash.parquet",
    "IN": "india_cash.parquet",
}
DATE_CANDIDATE_COLS = ("date", "datetime", "timestamp", "dt")
TICKER_CANDIDATE_COLS = ("ticker", "symbol")
BENCHMARK_FALLBACKS = {
    "US": ["SPY", "QQQ", "IWM"],
    "HK": ["2800.HK"],
    "IN": ["NIFTYBEES.NS"],
}



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



def run_strategy_v2(market: str, universe_frames: dict[str, pd.DataFrame], bench_df: pd.DataFrame, breadth_df: pd.DataFrame | None = None, config: dict | None = None,) -> dict[str, Any]:
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


    # ── Build portfolio params from market config ─────────────────────────
    portfolio_params = {
        "max_positions": cfg.get("max_positions", 8),
        "max_sector_weight": cfg.get("max_sector_weight", 0.35),
        "max_theme_names": cfg.get("max_theme_names", 2),
        "max_single_weight": cfg.get("max_single_weight", 0.20),
        "min_weight": cfg.get("min_weight", 0.04),
    }

    result = run_pipeline_v2(
        tradable_frames=tradable_frames,
        leadership_frames=leadership_frames,
        bench_df=bench_df,
        breadth_df=breadth_df,
        market=cfg["market"],
        portfolio_params=portfolio_params,
        config=config,
    )
    result["market_config_v2"] = cfg
    result["tradable_universe"] = sorted(tradable)
    result["leadership_universe"] = sorted(leadership)

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