#!/usr/bin/env python3
"""
scripts/run_strategy.py
-----------------------
Unified CLI for the CASH strategy system.

Three execution modes across three markets (US, HK, India):

  top-down   — Sector rotation with RS-based stock selection (US)
               or composite relative-strength ranking (HK, India).
               Answers: "Where is the smart money flowing?"

  bottom-up  — Per-ticker technical scoring pipeline via the
               orchestrator.  Scores every ticker on momentum,
               trend, volume, breadth, and relative strength.
               Answers: "Which individual stocks look strongest?"

  full       — Combined pipeline: bottom-up feeds indicator data
               into top-down rotation for quality-filtered stock
               selection, then convergence merges both signal
               lists for maximum conviction.
               Answers: "What should I buy, sell, or hold?"

Usage
=====

  # ── Top-Down (sector rotation / RS ranking) ────────
  python -m scripts.run_strategy top-down --market US
  python -m scripts.run_strategy top-down --market US --quality --holdings NVDA,CRWD
  python -m scripts.run_strategy top-down --market HK
  python -m scripts.run_strategy top-down --market IN

  # ── Bottom-Up (per-ticker scoring) ─────────────────
  python -m scripts.run_strategy bottom-up --market US
  python -m scripts.run_strategy bottom-up --market HK
  python -m scripts.run_strategy bottom-up --market IN

  # ── Full Pipeline (combined) ────────────────────────
  python -m scripts.run_strategy full --market US --holdings NVDA,CRWD,CEG
  python -m scripts.run_strategy full --market ALL
  python -m scripts.run_strategy full --market ALL -o results/report.json
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

# ── Ensure project root is importable ─────────────────────────
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from pipeline.orchestrator import (
    Orchestrator,
    PipelineResult,
    run_full_pipeline,
    run_multi_market_pipeline,
)
from strategy_phase1.rotation import (
    RotationConfig,
    RotationResult,
    composite_rs_all,
    run_rotation,
    print_result as print_rotation_result,
)
from strategy_phase1.rotation_filters import QualityConfig
from common.config import MARKET_CONFIG, ACTIVE_MARKETS, LOGS_DIR

# Optional: convergence module for price matrix building
try:
    from strategy_phase1.convergence import build_price_matrix as _conv_build_prices
except ImportError:
    _conv_build_prices = None

log = logging.getLogger("run_strategy")

W = 80  # print width


# ═══════════════════════════════════════════════════════════════
#  LOGGING SETUP
# ═══════════════════════════════════════════════════════════════

def _setup_logging(level: int) -> str:
    """
    Configure root logger with both console and file handlers.

    Writes structured logs to LOGS_DIR/run_strategy_<timestamp>.log
    for post-run diagnostics and strategy improvement.

    Returns the log file path.
    """
    logs_path = Path(LOGS_DIR)
    logs_path.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = str(logs_path / f"run_strategy_{ts}.log")

    fmt = "%(asctime)s │ %(name)-22s │ %(message)s"
    datefmt = "%H:%M:%S"

    # Clear any existing handlers (avoid duplicate output)
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level)

    # Console handler
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(level)
    console.setFormatter(logging.Formatter(fmt, datefmt=datefmt))
    root.addHandler(console)

    # File handler — always at DEBUG level for full diagnostics
    file_handler = logging.FileHandler(log_file, mode="w", encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_fmt = "%(asctime)s │ %(levelname)-8s │ %(name)-22s │ %(message)s"
    file_handler.setFormatter(logging.Formatter(file_fmt, datefmt="%H:%M:%S"))
    root.addHandler(file_handler)

    # Quiet noisy libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("yfinance").setLevel(logging.WARNING)

    return log_file


# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════

def _resolve_markets(market_arg: str) -> list[str]:
    """Parse --market flag into a list of market codes."""
    if market_arg.upper() == "ALL":
        return list(ACTIVE_MARKETS)
    code = market_arg.upper()
    if code not in MARKET_CONFIG:
        available = ", ".join(MARKET_CONFIG.keys())
        raise SystemExit(
            f"Unknown market '{code}'.  Available: {available}, ALL"
        )
    return [code]


def _parse_holdings(holdings_str: str) -> list[str]:
    """Parse comma-separated holdings string."""
    if not holdings_str:
        return []
    return [t.strip().upper() for t in holdings_str.split(",") if t.strip()]


def _build_price_matrix(ohlcv: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Build a wide close-price matrix from {ticker: OHLCV DataFrame}."""
    if _conv_build_prices is not None:
        return _conv_build_prices(ohlcv)

    series: dict[str, pd.Series] = {}
    for ticker, df in ohlcv.items():
        if df is not None and not df.empty and "close" in df.columns:
            series[ticker] = df["close"]
    if not series:
        return pd.DataFrame()
    return pd.DataFrame(series).sort_index().ffill()


def _compute_indicators_inline(
    ohlcv: dict[str, pd.DataFrame],
    tickers: list[str] | None = None,
) -> dict[str, pd.DataFrame]:
    """Compute technical indicators for quality filtering in top-down standalone mode."""
    try:
        from compute.indicators import compute_all_indicators
    except ImportError:
        log.warning("compute.indicators not importable — quality filter will be disabled")
        return {}

    if tickers is None:
        tickers = list(ohlcv.keys())

    result: dict[str, pd.DataFrame] = {}
    n_fail = 0

    for ticker in tickers:
        df = ohlcv.get(ticker)
        if df is None or df.empty or len(df) < 60:
            continue
        try:
            enriched = compute_all_indicators(df)
            if enriched is not None and not enriched.empty:
                result[ticker] = enriched
        except Exception as exc:
            log.debug("Indicators failed for %s: %s", ticker, exc)
            n_fail += 1

    log.info("Inline indicators: %d OK, %d failed out of %d", len(result), n_fail, len(tickers))
    return result


def _market_suffix(market: str) -> str:
    return {"HK": ".HK", "IN": ".NS"}.get(market, "")


def _is_market_ticker(ticker: str, market: str) -> bool:
    if market == "US":
        return not ticker.endswith(".HK") and not ticker.endswith(".NS")
    suffix = _market_suffix(market)
    return ticker.endswith(suffix) if suffix else True


# ═══════════════════════════════════════════════════════════════
#  SNAPSHOT VALUE EXTRACTORS  (FIX for Issue 2)
# ═══════════════════════════════════════════════════════════════

def _snap_composite(snap: dict) -> float:
    """Extract composite score from a snapshot dict, trying multiple keys."""
    for key in ("composite", "score_composite", "score_adjusted", "composite_score"):
        val = snap.get(key)
        if val is not None and val != 0:
            try:
                return float(val)
            except (TypeError, ValueError):
                pass
    return 0.0


def _snap_rs(snap: dict) -> float:
    """Extract RS z-score from a snapshot dict."""
    # Direct key
    for key in ("rs_zscore",):
        val = snap.get(key)
        if val is not None:
            try:
                return float(val)
            except (TypeError, ValueError):
                pass
    # Nested under "rs" dict
    rs_dict = snap.get("rs", {})
    if isinstance(rs_dict, dict):
        val = rs_dict.get("rs_zscore", rs_dict.get("rs_ratio"))
        if val is not None:
            try:
                return float(val)
            except (TypeError, ValueError):
                pass
    return 0.0


def _snap_rsi(snap: dict) -> float:
    """Extract RSI from a snapshot dict."""
    # Nested under "indicators" dict
    ind = snap.get("indicators", {})
    if isinstance(ind, dict):
        val = ind.get("rsi")
        if val is not None:
            try:
                return float(val)
            except (TypeError, ValueError):
                pass
    # Direct key fallbacks
    for key in ("rsi_14", "rsi"):
        val = snap.get(key)
        if val is not None:
            try:
                return float(val)
            except (TypeError, ValueError):
                pass
    return 0.0


def _snap_adx(snap: dict) -> float:
    """Extract ADX from a snapshot dict."""
    ind = snap.get("indicators", {})
    if isinstance(ind, dict):
        val = ind.get("adx")
        if val is not None:
            try:
                return float(val)
            except (TypeError, ValueError):
                pass
    for key in ("adx_14", "adx"):
        val = snap.get(key)
        if val is not None:
            try:
                return float(val)
            except (TypeError, ValueError):
                pass
    return 0.0


def _snap_signal(snap: dict) -> str:
    """Extract signal from a snapshot, checking convergence label first."""
    for key in ("convergence_label", "signal"):
        val = snap.get(key)
        if val and str(val) not in ("None", "NEUTRAL", ""):
            return str(val)
    return snap.get("signal", "HOLD")


# ═══════════════════════════════════════════════════════════════
#  TOP-DOWN MODE
# ═══════════════════════════════════════════════════════════════

def _run_top_down(args) -> dict[str, Any]:
    """Top-down analysis for requested markets."""
    markets = _resolve_markets(args.market)
    holdings = _parse_holdings(args.holdings)
    results: dict[str, Any] = {}

    for market in markets:
        mcfg = MARKET_CONFIG[market]
        benchmark = mcfg.get("benchmark", "SPY")
        engines = mcfg.get("engines", ["scoring"])
        t0 = time.perf_counter()

        _print_header(f"TOP-DOWN ANALYSIS  —  {market}", benchmark=benchmark)

        orch = Orchestrator(market=market, lookback_days=args.lookback)
        orch.load_data()

        prices = _build_price_matrix(orch._ohlcv)
        if prices.empty:
            log.error("[%s] Empty price matrix — skipping", market)
            continue
        if benchmark not in prices.columns:
            log.error("[%s] Benchmark %s not in price data — skipping", market, benchmark)
            continue

        n_days = len(prices)
        n_tickers = prices.shape[1]
        date_range = f"{prices.index[0].strftime('%Y-%m-%d')} to {prices.index[-1].strftime('%Y-%m-%d')}"
        print(f"  Data: {n_days} trading days × {n_tickers} tickers")
        print(f"  Range: {date_range}")

        if "rotation" in engines:
            indicator_data: dict[str, pd.DataFrame] | None = None
            if args.quality:
                print(f"\n  Computing indicators for quality filter...")
                indicator_data = _compute_indicators_inline(orch._ohlcv)
                if not indicator_data:
                    indicator_data = None
                    print(f"  ⚠  No indicator data — falling back to RS-only")
                else:
                    print(f"  ✓  Indicators for {len(indicator_data)} tickers")

            qcfg = QualityConfig(enabled=bool(args.quality and indicator_data))
            if args.quality_weight is not None:
                qcfg.w_quality = args.quality_weight
                qcfg.w_rs = 1.0 - args.quality_weight

            rcfg_kw: dict[str, Any] = {"benchmark": benchmark, "quality": qcfg}
            if args.stocks_per_sector is not None:
                rcfg_kw["stocks_per_sector"] = args.stocks_per_sector
            if args.max_positions is not None:
                rcfg_kw["max_total_positions"] = args.max_positions

            rcfg = RotationConfig(**rcfg_kw)
            rotation_result = run_rotation(prices=prices, current_holdings=holdings, config=rcfg, indicator_data=indicator_data)
            print_rotation_result(rotation_result)
            results[market] = rotation_result
        else:
            _print_rs_ranking(prices, benchmark, market)
            config = RotationConfig(benchmark=benchmark)
            rs_all, raw = composite_rs_all(prices, config)
            results[market] = {"rs_ranking": rs_all, "raw_returns": raw}

        elapsed = time.perf_counter() - t0
        print(f"\n  ⏱  {market} top-down: {elapsed:.1f}s")

    return results


def _print_rs_ranking(prices: pd.DataFrame, benchmark: str, market: str) -> None:
    """Print tiered RS ranking for non-rotation markets (HK, India)."""
    config = RotationConfig(benchmark=benchmark)
    rs_all, raw = composite_rs_all(prices, config)

    tickers = [t for t in rs_all.index if _is_market_ticker(t, market) and t != benchmark]
    if not tickers:
        print(f"\n  No tickers found for market {market}")
        return

    filtered = rs_all.loc[rs_all.index.isin(tickers)].sort_values(ascending=False)
    n_total = len(filtered)
    n_top = max(1, n_total // 3)
    n_bot = max(1, n_total // 3)

    print(f"\n  RELATIVE STRENGTH vs {benchmark}  ({n_total} tickers)")
    print(f"  {'─' * (W - 4)}")

    top = filtered.head(n_top)
    print(f"\n  🟢 TOP TIER  ({len(top)} tickers — strongest RS)")
    print(f"  {'─' * (W - 4)}")
    for i, (ticker, rs) in enumerate(top.items()):
        rets = raw.get(ticker, {})
        rets_str = "  ".join(f"{p}d:{r:+.1%}" for p, r in sorted(rets.items()))
        print(f"   {i + 1:3d}. {ticker:16s}  RS {rs:+.4f}   {rets_str}")

    mid_start = n_top
    mid_end = n_total - n_bot
    mid = filtered.iloc[mid_start:mid_end]
    if not mid.empty:
        print(f"\n  ⚪ MIDDLE TIER  ({len(mid)} tickers)")
        print(f"  {'─' * (W - 4)}")
        for i, (ticker, rs) in enumerate(mid.items()):
            rets = raw.get(ticker, {})
            rets_str = "  ".join(f"{p}d:{r:+.1%}" for p, r in sorted(rets.items()))
            print(f"   {mid_start + i + 1:3d}. {ticker:16s}  RS {rs:+.4f}   {rets_str}")

    bot = filtered.tail(n_bot)
    if not bot.empty:
        print(f"\n  🔴 BOTTOM TIER  ({len(bot)} tickers — weakest RS)")
        print(f"  {'─' * (W - 4)}")
        for i, (ticker, rs) in enumerate(bot.items()):
            rets = raw.get(ticker, {})
            rets_str = "  ".join(f"{p}d:{r:+.1%}" for p, r in sorted(rets.items()))
            idx = n_total - n_bot + i + 1
            print(f"   {idx:3d}. {ticker:16s}  RS {rs:+.4f}   {rets_str}")

    print(f"\n  {'─' * (W - 4)}")
    bench_ret = raw.get(benchmark, {})
    bench_str = "  ".join(f"{p}d:{r:+.1%}" for p, r in sorted(bench_ret.items()))
    print(f"  Benchmark {benchmark}: {bench_str}")
    print(f"{'═' * W}")


# ═══════════════════════════════════════════════════════════════
#  BOTTOM-UP MODE
# ═══════════════════════════════════════════════════════════════

def _run_bottom_up(args) -> dict[str, PipelineResult]:
    """Per-ticker scoring pipeline for requested markets."""
    markets = _resolve_markets(args.market)
    results: dict[str, PipelineResult] = {}

    for market in markets:
        mcfg = MARKET_CONFIG[market]
        t0 = time.perf_counter()

        _print_header(
            f"BOTTOM-UP SCORING  —  {market}",
            benchmark=mcfg.get("benchmark", "?"),
            extra=f"Universe: {len(mcfg.get('universe', []))} tickers",
        )

        orch = Orchestrator(market=market, lookback_days=args.lookback, enable_backtest=False)
        orch.load_data()
        orch.compute_universe_context()
        orch.run_tickers()
        orch.cross_sectional_analysis()
        result = orch.generate_reports()

        _print_bottom_up_result(result, market)
        results[market] = result

        elapsed = time.perf_counter() - t0
        print(f"\n  ⏱  {market} bottom-up: {elapsed:.1f}s total")

    return results


def _print_bottom_up_result(
    result: PipelineResult,
    market: str,
    top_n: int = 15,
) -> None:
    """Pretty-print bottom-up scoring results (FIXED key access)."""
    snaps = result.snapshots
    if not snaps:
        print("\n  No scored tickers.")
        return

    # ── Pipeline stats ────────────────────────────────────
    print(f"\n  Scored: {result.n_tickers}  │  "
          f"Errors: {result.n_errors}  │  "
          f"Compute time: {result.total_time:.1f}s")

    # ── Breadth regime ────────────────────────────────────
    if result.breadth is not None and not result.breadth.empty:
        if "breadth_regime" in result.breadth.columns:
            regime = result.breadth["breadth_regime"].iloc[-1]
            print(f"  Breadth regime: {regime}")

    # ── Top N tickers (FIXED: use correct snapshot keys) ──
    show_n = min(top_n, len(snaps))
    print(f"\n  TOP {show_n} SCORED TICKERS")
    print(f"  {'─' * (W - 4)}")
    print(
        f"  {'#':>3s}  {'Ticker':10s} {'Score':>6s}  "
        f"{'Signal':>9s}  {'RS':>8s}  {'RSI':>6s}  {'ADX':>6s}"
    )
    print(f"  {'─' * (W - 4)}")

    for i, snap in enumerate(snaps[:show_n]):
        ticker = snap.get("ticker", "?")
        score = _snap_composite(snap)
        signal = _snap_signal(snap)
        rs = _snap_rs(snap)
        rsi = _snap_rsi(snap)
        adx = _snap_adx(snap)

        sig_icon = {
            "BUY": "🟢", "STRONG_BUY": "🟢", "BUY_SCORING": "🟢",
            "BUY_ROTATION": "🟢",
            "SELL": "🔴", "STRONG_SELL": "🔴", "SELL_SCORING": "🔴",
            "SELL_ROTATION": "🔴",
            "HOLD": "⚪", "NEUTRAL": "⚪",
        }.get(str(signal), "⚪")

        # Shorten signal label for display
        sig_display = str(signal).replace("_SCORING", "").replace("_ROTATION", "(R)")
        if sig_display == "STRONG_BUY":
            sig_display = "S.BUY"
        elif sig_display == "STRONG_SELL":
            sig_display = "S.SELL"

        print(
            f"  {i + 1:3d}  {ticker:10s} {score:6.3f}  "
            f"{sig_icon}{sig_display:>7s}  "
            f"{rs:+8.4f}  {rsi:6.1f}  {adx:6.1f}"
        )

    # ── Signal summary ────────────────────────────────────
    buys = [s for s in snaps if _snap_signal(s) in ("BUY", "STRONG_BUY", "BUY_SCORING", "BUY_ROTATION")]
    sells = [s for s in snaps if _snap_signal(s) in ("SELL", "STRONG_SELL", "SELL_SCORING", "SELL_ROTATION")]
    holds = len(snaps) - len(buys) - len(sells)

    print(f"\n  {'─' * (W - 4)}")
    print(
        f"  Signals: {len(buys)} BUY  │  {holds} HOLD  │  "
        f"{len(sells)} SELL  │  {len(snaps)} total"
    )

    # ── Show SELL tickers explicitly (FIX for Issue 4) ────
    if sells:
        sell_tickers = ", ".join(s.get("ticker", "?") for s in sells[:10])
        print(f"  Sells:   {sell_tickers}")

    # ── Portfolio summary ─────────────────────────────────
    if result.portfolio:
        meta = result.portfolio.get("metadata", {})
        n_pos = meta.get("num_holdings", 0)
        cash_pct = meta.get("cash_pct", 0)
        print(
            f"  Portfolio: {n_pos} positions  │  "
            f"Cash: {cash_pct:.1%}"
        )

    print(f"{'═' * W}")


# ═══════════════════════════════════════════════════════════════
#  FULL MODE
# ═══════════════════════════════════════════════════════════════

def _run_full(args) -> dict[str, PipelineResult]:
    """Full combined pipeline for requested markets."""
    markets = _resolve_markets(args.market)
    holdings = _parse_holdings(args.holdings)
    results: dict[str, PipelineResult] = {}

    if len(markets) > 1:
        holdings_map: dict[str, list[str]] | None = None
        if holdings:
            holdings_map = {m: [] for m in markets}
            holdings_map["US"] = holdings

        raw = run_multi_market_pipeline(
            active_markets=markets,
            lookback_days=args.lookback,
            current_holdings=holdings_map,
        )
        for market, result in raw.items():
            _print_full_result(result, market)
            results[market] = result
    else:
        market = markets[0]
        result = run_full_pipeline(
            market=market,
            lookback_days=args.lookback,
            current_holdings=holdings,
        )
        _print_full_result(result, market)
        results[market] = result

    return results


def _print_full_result(result: PipelineResult, market: str) -> None:
    """Pretty-print full pipeline results."""
    _print_header(f"FULL PIPELINE  —  {market}", extra=result.summary())

    _print_bottom_up_result(result, market, top_n=10)

    if result.rotation_result is not None:
        print(f"\n{'─' * W}")
        print(f"  SECTOR ROTATION OVERLAY")
        print(f"{'─' * W}")
        print_rotation_result(result.rotation_result)

    if result.convergence is not None:
        _print_convergence(result.convergence)

    print(f"\n{'═' * W}")


def _print_convergence(conv: Any) -> None:
    """Print convergence merge results."""
    print(f"\n{'─' * W}")
    print(f"  CONVERGENCE MERGE")
    print(f"{'─' * W}")

    strong = getattr(conv, "strong_buys", [])
    if strong:
        print(f"\n  🟢 STRONG BUYS  ({len(strong)})")
        for sb in strong[:10]:
            ticker = getattr(sb, "ticker", str(sb))
            adj = getattr(sb, "adjusted_score", None)
            score_str = f"  adj={adj:.3f}" if adj is not None else ""
            print(f"     {ticker}{score_str}")
        if len(strong) > 10:
            print(f"     ... and {len(strong) - 10} more")

    conflicts = getattr(conv, "conflicts", [])
    if conflicts:
        print(f"\n  ⚠  CONFLICTS  ({len(conflicts)})")
        for c in conflicts[:5]:
            ticker = getattr(c, "ticker", str(c))
            reason = getattr(c, "reason", "")
            reason_str = f"  — {reason}" if reason else ""
            print(f"     {ticker}{reason_str}")

    summary_fn = getattr(conv, "summary", None)
    if callable(summary_fn):
        print(f"\n  {summary_fn()}")


# ═══════════════════════════════════════════════════════════════
#  PRINTING HELPERS
# ═══════════════════════════════════════════════════════════════

def _print_header(title: str, benchmark: str | None = None, extra: str | None = None) -> None:
    print(f"\n{'═' * W}")
    print(f"  {title}  —  {date.today()}")
    if benchmark:
        print(f"  Benchmark: {benchmark}")
    if extra:
        print(f"  {extra}")
    print(f"{'═' * W}")


# ═══════════════════════════════════════════════════════════════
#  JSON EXPORT  (FIXED: correct keys + market-aware filenames)
# ═══════════════════════════════════════════════════════════════

def _export_json(
    results: dict[str, Any],
    output_path: str,
    mode: str,
) -> None:
    """
    Serialise results to JSON.

    When the output path ends with .json AND multiple markets
    are present, generates one file per market with the pattern:
      result_<MARKET>_<DATE>.json

    When a single market is present and -o specifies a concrete
    file, uses that filename but injects market and date.
    """
    today_str = date.today().strftime("%Y%m%d")
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)

    for market, result in results.items():
        # Build per-market filename: result_US_20260423.json
        if len(results) > 1 or p.name == "report.json":
            market_file = p.parent / f"result_{market}_{today_str}.json"
        else:
            # Single market: inject market+date into filename
            stem = p.stem
            market_file = p.parent / f"result_{market}_{today_str}.json"

        data: dict[str, Any] = {
            "mode": mode,
            "market": market,
            "run_date": str(date.today()),
        }

        if isinstance(result, RotationResult):
            data.update(_rotation_to_dict(result))
        elif isinstance(result, PipelineResult):
            data.update(_pipeline_to_dict(result))
        elif isinstance(result, dict) and "rs_ranking" in result:
            data.update(_rs_ranking_to_dict(result))

        with open(market_file, "w") as f:
            json.dump(data, f, indent=2, default=str)

        print(f"\n  📄 Exported to {market_file}")


def _rotation_to_dict(rr: RotationResult) -> dict:
    return {
        "type": "rotation",
        "as_of": str(rr.as_of_date),
        "leading_sectors": rr.leading_sectors,
        "lagging_sectors": rr.lagging_sectors,
        "quality_stats": rr.quality_stats,
        "sector_rankings": [
            {
                "rank": s.rank,
                "sector": s.sector,
                "etf": s.etf,
                "tier": s.tier,
                "composite_rs": round(s.composite_rs, 6),
                "period_returns": {str(k): round(v, 6) for k, v in s.period_returns.items()},
            }
            for s in rr.sector_rankings
        ],
        "recommendations": [
            {
                "ticker": r.ticker,
                "action": r.action.value,
                "sector": r.sector,
                "sector_rank": r.sector_rank,
                "sector_tier": r.sector_tier,
                "rs_composite": round(r.rs_composite, 6),
                "rs_vs_sector_etf": round(r.rs_vs_sector_etf, 6),
                "quality_score": round(r.quality_score, 4),
                "quality_gate_passed": r.quality_gate_passed,
                "blended_score": round(r.blended_score, 4),
                "reason": r.reason,
            }
            for r in rr.recommendations
        ],
        "summary": {
            "buys": len(rr.buys),
            "sells": len(rr.sells),
            "reduces": len(rr.reduces),
            "holds": len(rr.holds),
        },
    }


def _pipeline_to_dict(result: PipelineResult) -> dict:
    """Serialise a PipelineResult for JSON (FIXED: correct snapshot keys)."""
    out: dict[str, Any] = {
        "type": "pipeline",
        "n_tickers": result.n_tickers,
        "n_errors": result.n_errors,
        "total_time": round(result.total_time, 2),
        "top_30": [],
    }

    for s in result.snapshots[:30]:
        entry = {
            "ticker": s.get("ticker"),
            "composite_score": round(_snap_composite(s), 4),
            "signal": _snap_signal(s),
            "rs_zscore": round(_snap_rs(s), 4),
            "rsi": round(_snap_rsi(s), 2),
            "adx": round(_snap_adx(s), 2),
        }

        # Include pillar scores if available
        for key in ("score_rotation", "score_momentum", "score_volatility",
                     "score_microstructure", "score_breadth"):
            val = s.get(key)
            if val is not None:
                entry[key] = round(float(val), 4)

        # Include convergence data if available
        for key in ("convergence_label", "adjusted_score", "rotation_signal",
                     "rotation_sector", "rotation_sector_rank"):
            val = s.get(key)
            if val is not None:
                entry[key] = val if not isinstance(val, float) else round(val, 4)

        out["top_30"].append(entry)

    if result.rotation_result is not None:
        rr = result.rotation_result
        out["rotation"] = {
            "leading_sectors": rr.leading_sectors,
            "lagging_sectors": rr.lagging_sectors,
            "buys": [r.ticker for r in rr.buys],
            "sells": [r.ticker for r in rr.sells],
        }

    if result.convergence is not None:
        conv = result.convergence
        strong = getattr(conv, "strong_buys", [])
        out["convergence"] = {
            "strong_buys": [getattr(sb, "ticker", str(sb)) for sb in strong],
            "n_conflicts": len(getattr(conv, "conflicts", [])),
        }

    return out


def _rs_ranking_to_dict(result: dict) -> dict:
    rs = result.get("rs_ranking")
    if rs is None:
        return {"type": "rs_ranking", "tickers": []}
    return {
        "type": "rs_ranking",
        "tickers": [{"ticker": t, "rs": round(float(v), 6)} for t, v in rs.items()],
    }


# ═══════════════════════════════════════════════════════════════
#  ARGUMENT PARSER
# ═══════════════════════════════════════════════════════════════

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="run_strategy",
        description="CASH Strategy System — unified CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  %(prog)s top-down --market US
  %(prog)s top-down --market US --quality --holdings NVDA,CRWD
  %(prog)s bottom-up --market US
  %(prog)s full --market US --holdings NVDA,CRWD,CEG
  %(prog)s full --market ALL -o results/
""",
    )

    sub = p.add_subparsers(dest="mode", required=True)

    def _add_common(sp: argparse.ArgumentParser) -> None:
        sp.add_argument("--market", default="US", help="Market code: US, HK, IN, or ALL (default: US)")
        sp.add_argument("--lookback", type=int, default=None, metavar="DAYS", help="Calendar days of history to load.")
        sp.add_argument("--holdings", default="", help="Comma-separated current holdings")
        sp.add_argument("--output", "-o", metavar="PATH", help="Export results to JSON file")
        sp.add_argument("--verbose", "-v", action="store_true", help="Debug-level logging")
        sp.add_argument("--quiet", "-q", action="store_true", help="Suppress log output")

    td = sub.add_parser("top-down", help="Sector rotation (US) / RS ranking (HK, IN)")
    _add_common(td)
    td.add_argument("--quality", action="store_true", help="Enable quality filter for US rotation")
    td.add_argument("--quality-weight", type=float, metavar="W", help="Quality weight in blend (default: 0.40)")
    td.add_argument("--stocks-per-sector", type=int, metavar="N", help="Stock picks per leading sector")
    td.add_argument("--max-positions", type=int, metavar="N", help="Maximum total portfolio positions")

    bu = sub.add_parser("bottom-up", help="Per-ticker scoring pipeline")
    _add_common(bu)

    fu = sub.add_parser("full", help="Combined bottom-up + top-down pipeline")
    _add_common(fu)

    return p


# ═══════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════

def main(argv: list[str] | None = None) -> dict[str, Any]:
    parser = _build_parser()
    args = parser.parse_args(argv)

    # ── Logging (FIX: write to LOGS_DIR) ──────────────────
    level = (
        logging.DEBUG if args.verbose
        else logging.WARNING if args.quiet
        else logging.INFO
    )
    log_file = _setup_logging(level)

    t0 = time.perf_counter()

    log.info(f"Strategy run: mode={args.mode}, market={args.market}")
    log.info(f"Log file: {log_file}")

    # ── Dispatch ──────────────────────────────────────────
    if args.mode == "top-down":
        results = _run_top_down(args)
    elif args.mode == "bottom-up":
        results = _run_bottom_up(args)
    elif args.mode == "full":
        results = _run_full(args)
    else:
        parser.print_help()
        return {}

    # ── Export (FIX: market-aware filenames) ───────────────
    if args.output:
        _export_json(results, args.output, args.mode)

    elapsed = time.perf_counter() - t0
    n_markets = len(results)
    print(f"\n  ✓  Done — {n_markets} market(s) in {elapsed:.1f}s")
    log.info(f"Completed in {elapsed:.1f}s — log at {log_file}")
    print()

    return results


if __name__ == "__main__":
    main()