"""
backtest/phase2/data_cache.py
Cache pre-computed indicator frames to avoid recomputing on every run.

Cache key: hash of (market, ticker list, parquet file mtime+size, lookback).
Strategy config changes do NOT invalidate the cache — only data changes do.

Usage in engine:
    from backtest.phase2.data_cache import load_or_compute_indicators
    self._precomputed_frames, self._precomputed_regime_df = load_or_compute_indicators(
        data_source=self.data_source,
        market=self.market,
        vol_regime_params=self.config.get("vol_regime_params"),
        cache_dir=".cache/backtest",
        invalidate=False,
    )
"""
from __future__ import annotations

import hashlib
import logging
import pickle
import time
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd

log = logging.getLogger(__name__)

CACHE_DIR_DEFAULT = Path(".cache") / "backtest"


def _compute_cache_key(
    market: str,
    tickers: list[str],
    data_source,
) -> str:
    """
    Build a deterministic cache key from:
      - market name
      - sorted ticker list
      - date range of loaded data (proxy for data content)
      - number of tickers loaded
    
    If the parquet file changes (new data, different tickers), the
    date range or ticker count will differ → cache miss.
    """
    lo, hi = data_source.get_date_range()
    loaded_tickers = sorted(data_source.get_tickers())
    
    # Build a string that uniquely identifies this data state
    key_parts = [
        f"market={market}",
        f"n_tickers={len(loaded_tickers)}",
        f"date_range={lo.strftime('%Y%m%d')}_{hi.strftime('%Y%m%d')}",
        f"tickers_hash={hashlib.md5('|'.join(loaded_tickers).encode()).hexdigest()[:12]}",
    ]
    key_str = "__".join(key_parts)
    return hashlib.sha256(key_str.encode()).hexdigest()[:20]


def _compute_indicators(
    data_source,
    vol_regime_params: Optional[dict],
) -> Tuple[Dict[str, pd.DataFrame], Optional[pd.DataFrame]]:
    """
    The actual computation — extracted from engine._precompute_all().
    Returns (precomputed_frames, regime_df).
    """
    from refactor.pipeline_v2 import (
        _canonicalize_indicator_columns,
        _fill_missing_indicators,
        annotate_scoreability,
    )
    from refactor.strategy.adapters_v2 import ensure_columns
    from refactor.strategy.regime_v2 import classify_volatility_regime
    from refactor.strategy.rs_v2 import compute_rs_zscores, enrich_rs_regimes
    from compute.indicators import compute_all_indicators

    t0 = time.time()

    # 1 — per-ticker indicators
    raw_frames = {}
    for ticker, df in data_source.ticker_data.items():
        if df is not None and not df.empty:
            enriched = compute_all_indicators(df.copy())
            enriched = _canonicalize_indicator_columns(enriched)
            enriched = _fill_missing_indicators(enriched)
            enriched = ensure_columns(enriched)
            raw_frames[ticker] = enriched

    log.info(
        "Cache: computed indicators for %d tickers (%.1fs)",
        len(raw_frames), time.time() - t0,
    )

    # 2 — cross-sectional RS z-scores + regimes
    t1 = time.time()
    regime_df = None
    bench_df = data_source.benchmark_data
    if bench_df is not None and not bench_df.empty:
        raw_frames = compute_rs_zscores(raw_frames, bench_df)
        raw_frames = enrich_rs_regimes(raw_frames)
        regime_df = classify_volatility_regime(
            bench_df,
            params=vol_regime_params,
        )
    log.info(
        "Cache: computed RS + regimes (%.1fs)",
        time.time() - t1,
    )

    # 3 — annotate scoreability
    for ticker in raw_frames:
        raw_frames[ticker] = annotate_scoreability(raw_frames[ticker])

    log.info(
        "Cache: pre-computation complete: %d tickers, total %.1fs",
        len(raw_frames), time.time() - t0,
    )

    return raw_frames, regime_df


def load_or_compute_indicators(
    data_source,
    market: str,
    vol_regime_params: Optional[dict] = None,
    cache_dir: str | Path = CACHE_DIR_DEFAULT,
    invalidate: bool = False,
) -> Tuple[Dict[str, pd.DataFrame], Optional[pd.DataFrame]]:
    """
    Load cached pre-computed indicators, or compute and cache them.

    Parameters
    ----------
    data_source : BacktestDataSource
        The loaded data source.
    market : str
        Market code (e.g. "HK", "US").
    vol_regime_params : dict, optional
        Volatility regime params (used in classify_volatility_regime).
        NOTE: if you change these, pass invalidate=True.
    cache_dir : Path
        Directory for cache files.
    invalidate : bool
        If True, recompute even if cache exists.

    Returns
    -------
    (precomputed_frames, regime_df)
    """
    cache_path = Path(cache_dir)
    cache_path.mkdir(parents=True, exist_ok=True)

    cache_key = _compute_cache_key(market, data_source.get_tickers(), data_source)
    frames_file = cache_path / f"indicators_{market}_{cache_key}.pkl"
    regime_file = cache_path / f"regime_{market}_{cache_key}.pkl"

    # ── Try loading from cache ────────────────────────────────────
    if not invalidate and frames_file.exists() and regime_file.exists():
        t0 = time.time()
        try:
            with open(frames_file, "rb") as f:
                precomputed_frames = pickle.load(f)
            with open(regime_file, "rb") as f:
                regime_df = pickle.load(f)

            log.info(
                "Cache HIT: loaded %d ticker frames from %s (%.1fs)",
                len(precomputed_frames),
                frames_file.name,
                time.time() - t0,
            )
            return precomputed_frames, regime_df

        except Exception as exc:
            log.warning(
                "Cache load failed (%s), recomputing: %s",
                frames_file.name, exc,
            )

    # ── Compute fresh ─────────────────────────────────────────────
    log.info("Cache MISS: computing indicators for %s...", market)
    precomputed_frames, regime_df = _compute_indicators(
        data_source, vol_regime_params
    )

    # ── Save to cache ─────────────────────────────────────────────
    t0 = time.time()
    try:
        with open(frames_file, "wb") as f:
            pickle.dump(precomputed_frames, f, protocol=pickle.HIGHEST_PROTOCOL)
        with open(regime_file, "wb") as f:
            pickle.dump(regime_df, f, protocol=pickle.HIGHEST_PROTOCOL)

        size_mb = frames_file.stat().st_size / (1024 * 1024)
        log.info(
            "Cache SAVED: %s (%.1f MB, %.1fs)",
            frames_file.name, size_mb, time.time() - t0,
        )
    except Exception as exc:
        log.warning("Cache save failed: %s", exc)

    return precomputed_frames, regime_df


def clear_cache(market: Optional[str] = None, cache_dir: str | Path = CACHE_DIR_DEFAULT):
    """Delete cached indicator files."""
    cache_path = Path(cache_dir)
    if not cache_path.exists():
        return

    pattern = f"*_{market}_*.pkl" if market else "*.pkl"
    removed = 0
    for f in cache_path.glob(pattern):
        f.unlink()
        removed += 1

    log.info("Cache cleared: removed %d files (pattern=%s)", removed, pattern)