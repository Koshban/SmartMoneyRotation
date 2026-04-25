"""
refactor/strategy/rs_v2.py

Cross-sectional relative-strength z-score computation and RRG-style regime classification.

Step 1 – compute_rs_zscores():
    Builds a date × symbol panel of rolling log returns, subtracts the
    benchmark return to get relative returns, then z-scores across the
    full cross-section on every date.  Writes rszscore, rsaccel20, and
    sectrszscore back into each per-symbol frame.

Step 2 – enrich_rs_regimes():
    Applies quadrant logic (level × momentum) to classify each symbol's
    rszscore trajectory into one of four regimes: leading, improving,
    weakening, lagging.  Same for sectrszscore → sectrsregime.
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ── Defaults ──────────────────────────────────────────────────────────────────
RS_RETURN_LOOKBACK = 20     # rolling return window (trading days)
RS_ACCEL_DIFF = 5           # window for Δ(rszscore) → rsaccel20
RS_REGIME_MA_WINDOW = 10    # rolling-MA window for quadrant classification
RS_MIN_HISTORY = 30         # minimum rows for a symbol to enter the panel


# ═══════════════════════════════════════════════════════════════════════════════
# Step 1 – cross-sectional z-scores
# ═══════════════════════════════════════════════════════════════════════════════

def compute_rs_zscores(
    symbol_frames: dict[str, pd.DataFrame],
    bench_df: pd.DataFrame,
    lookback: int = RS_RETURN_LOOKBACK,
    accel_diff: int = RS_ACCEL_DIFF,
    min_history: int = RS_MIN_HISTORY,
) -> dict[str, pd.DataFrame]:
    """
    Compute cross-sectional relative-strength z-scores for every symbol.

    For each trading date with sufficient data:
      1. Compute ``lookback``-period log return for the benchmark.
      2. Compute ``lookback``-period log return for each symbol.
      3. relative_return = symbol_return − benchmark_return
      4. Z-score relative returns across the symbol cross-section.
      5. rsaccel20 = diff(rszscore, ``accel_diff``) — short-term RS momentum.
      6. If ≥ 2 real sectors exist, compute sector-level z-scores.

    Returns a new dict with the same keys.  Each frame is a copy of the
    original with ``rszscore``, ``rsaccel20``, and ``sectrszscore``
    columns added or overwritten.
    """
    if not symbol_frames:
        logger.warning("compute_rs_zscores: no symbol frames provided; returning unchanged")
        return symbol_frames

    # ── 1. benchmark return series ────────────────────────────────────────────
    bench_close = pd.to_numeric(bench_df["close"], errors="coerce")
    bench_ret = np.log(bench_close / bench_close.shift(lookback))
    logger.info(
        "compute_rs_zscores: bench_rows=%d lookback=%d bench_ret_valid=%d",
        len(bench_df), lookback, int(bench_ret.notna().sum()),
    )

    # ── 2. build close-price panel and sector map ─────────────────────────────
    close_dict: dict[str, pd.Series] = {}
    sector_dict: dict[str, str] = {}

    for ticker, df in symbol_frames.items():
        if df is None or df.empty or "close" not in df.columns:
            continue
        if len(df) < min_history:
            logger.debug(
                "compute_rs_zscores: excluding %s (rows=%d < min_history=%d)",
                ticker, len(df), min_history,
            )
            continue
        close_dict[ticker] = pd.to_numeric(df["close"], errors="coerce")
        if "sector" in df.columns:
            vals = df["sector"].dropna().astype(str)
            sector_dict[ticker] = str(vals.iloc[-1]) if not vals.empty else "Unknown"
        else:
            sector_dict[ticker] = "Unknown"

    if not close_dict:
        logger.warning("compute_rs_zscores: no symbols with valid close data; returning unchanged")
        return {t: f.copy() for t, f in symbol_frames.items()}

    close_panel = pd.DataFrame(close_dict)
    logger.info(
        "compute_rs_zscores: close_panel shape=(%d dates, %d symbols)",
        close_panel.shape[0], close_panel.shape[1],
    )

    # ── 3. symbol log-returns and relative returns ────────────────────────────
    symbol_ret = np.log(close_panel / close_panel.shift(lookback))
    bench_ret_aligned = bench_ret.reindex(symbol_ret.index)
    relative_ret = symbol_ret.sub(bench_ret_aligned, axis=0)

    # ── 4. cross-sectional z-score per date ───────────────────────────────────
    cross_mean = relative_ret.mean(axis=1)
    cross_std = relative_ret.std(axis=1, ddof=1)
    cross_std = cross_std.where(cross_std > 1e-10, np.nan)
    zscore_panel = relative_ret.sub(cross_mean, axis=0).div(cross_std, axis=0)

    # ── 5. RS acceleration ────────────────────────────────────────────────────
    rs_accel_panel = zscore_panel.diff(accel_diff)

    # ── 6. sector-level z-scores ──────────────────────────────────────────────
    sector_zscore_panel = _compute_sector_zscores(relative_ret, sector_dict)

    # ── 7. write back into per-symbol frames ──────────────────────────────────
    enriched: dict[str, pd.DataFrame] = {}
    for ticker, df in symbol_frames.items():
        out = df.copy()
        if ticker in zscore_panel.columns:
            out["rszscore"] = zscore_panel[ticker].reindex(out.index)
            out["rsaccel20"] = rs_accel_panel[ticker].reindex(out.index)
        else:
            if "rszscore" not in out.columns:
                out["rszscore"] = np.nan
            if "rsaccel20" not in out.columns:
                out["rsaccel20"] = 0.0

        if sector_zscore_panel is not None and ticker in sector_zscore_panel.columns:
            out["sectrszscore"] = sector_zscore_panel[ticker].reindex(out.index)
        elif "sectrszscore" not in out.columns:
            out["sectrszscore"] = np.nan

        enriched[ticker] = out

    # ── diagnostics ───────────────────────────────────────────────────────────
    valid_per_date = zscore_panel.notna().sum(axis=1)
    last_zscores = (
        zscore_panel.iloc[-1].dropna()
        if not zscore_panel.empty
        else pd.Series(dtype=float)
    )
    logger.info(
        "compute_rs_zscores: panel_dates=%d symbols_in_panel=%d "
        "avg_valid_per_date=%.1f last_date_valid=%d",
        len(zscore_panel),
        len(close_dict),
        float(valid_per_date.mean()) if not valid_per_date.empty else 0.0,
        len(last_zscores),
    )
    if not last_zscores.empty:
        logger.info(
            "compute_rs_zscores last-date z-scores: "
            "min=%.4f p25=%.4f median=%.4f p75=%.4f max=%.4f std=%.4f",
            float(last_zscores.min()),
            float(last_zscores.quantile(0.25)),
            float(last_zscores.median()),
            float(last_zscores.quantile(0.75)),
            float(last_zscores.max()),
            float(last_zscores.std()),
        )
        if logger.isEnabledFor(logging.DEBUG):
            ranked = last_zscores.sort_values(ascending=False)
            logger.debug("compute_rs_zscores top-10 RS:\n%s", ranked.head(10).to_string())
            logger.debug("compute_rs_zscores bottom-10 RS:\n%s", ranked.tail(10).to_string())

    last_accel = (
        rs_accel_panel.iloc[-1].dropna()
        if not rs_accel_panel.empty
        else pd.Series(dtype=float)
    )
    if not last_accel.empty:
        logger.info(
            "compute_rs_zscores last-date rsaccel: "
            "min=%.4f median=%.4f max=%.4f",
            float(last_accel.min()),
            float(last_accel.median()),
            float(last_accel.max()),
        )

    return enriched


def _compute_sector_zscores(
    relative_ret: pd.DataFrame,
    sector_dict: dict[str, str],
) -> pd.DataFrame | None:
    """
    For each date, average the relative returns within each sector, then
    z-score the sector averages across sectors.  Map each symbol to its
    sector's z-score.

    Returns ``None`` if fewer than 2 real (non-Unknown) sectors exist.
    """
    if not sector_dict:
        return None

    sectors = pd.Series(sector_dict)
    real_sectors = sorted(
        s for s in sectors.unique() if s not in ("Unknown", "unknown", "")
    )
    if len(real_sectors) < 2:
        logger.info(
            "_compute_sector_zscores: %d real sector(s) found; need >= 2, skipping",
            len(real_sectors),
        )
        return None

    # per-sector average relative return per date
    sector_avg_dict: dict[str, pd.Series] = {}
    for sector in sectors.unique():
        members = sectors[sectors == sector].index.tolist()
        members_in_panel = [m for m in members if m in relative_ret.columns]
        if members_in_panel:
            sector_avg_dict[sector] = relative_ret[members_in_panel].mean(axis=1)

    sector_avg = pd.DataFrame(sector_avg_dict)
    if sector_avg.shape[1] < 2:
        return None

    sect_mean = sector_avg.mean(axis=1)
    sect_std = sector_avg.std(axis=1, ddof=1).replace(0, np.nan)
    sector_zscore = sector_avg.sub(sect_mean, axis=0).div(sect_std, axis=0)

    # map each symbol to its sector's z-score
    result = pd.DataFrame(index=relative_ret.index)
    for ticker, sector in sector_dict.items():
        if sector in sector_zscore.columns:
            result[ticker] = sector_zscore[sector]
        else:
            result[ticker] = np.nan

    logger.info(
        "_compute_sector_zscores: sectors=%d symbols_mapped=%d",
        len(sector_zscore.columns), len(result.columns),
    )
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# Step 2 – RRG-style regime classification
# ═══════════════════════════════════════════════════════════════════════════════

def classify_rs_regime(
    df: pd.DataFrame,
    rs_col: str = "rszscore",
    regime_col: str = "rsregime",
    ma_window: int = RS_REGIME_MA_WINDOW,
) -> pd.DataFrame:
    """
    Classify a single symbol's RS trajectory using quadrant logic.

    The two axes are *level* (above/below zero) and *momentum*
    (above/below a rolling MA of the z-score):

    ============  =============================  ==============================
    Quadrant      Condition                      Interpretation
    ============  =============================  ==============================
    leading       rs > 0  AND  rs > MA(rs)       strong & strengthening
    improving     rs <= 0 AND  rs > MA(rs)       weak but strengthening
    weakening     rs > 0  AND  rs <= MA(rs)      strong but fading
    lagging       rs <= 0 AND  rs <= MA(rs)      weak & fading
    ============  =============================  ==============================

    Rows where the z-score or its MA is NaN are labelled ``'unknown'``.
    """
    out = df.copy()

    if rs_col not in out.columns:
        out[regime_col] = "unknown"
        return out

    rs = pd.to_numeric(out[rs_col], errors="coerce")
    rs_ma = rs.rolling(ma_window, min_periods=max(3, ma_window // 2)).mean()

    above_zero = rs > 0
    above_ma = rs > rs_ma

    regime = pd.Series(
        np.select(
            [
                above_zero & above_ma,
                ~above_zero & above_ma,
                above_zero & ~above_ma,
                ~above_zero & ~above_ma,
            ],
            ["leading", "improving", "weakening", "lagging"],
            default="unknown",
        ),
        index=out.index,
    )

    # rows where the underlying data isn't ready → unknown
    regime = regime.where(rs.notna(), "unknown")
    regime = regime.where(rs_ma.notna(), "unknown")

    out[regime_col] = regime
    return out


def enrich_rs_regimes(
    symbol_frames: dict[str, pd.DataFrame],
    ma_window: int = RS_REGIME_MA_WINDOW,
) -> dict[str, pd.DataFrame]:
    """
    Apply RS regime classification to every symbol frame.

    Adds / overwrites:
        rsregime      — stock-level RS quadrant   (from rszscore)
        sectrsregime  — sector-level RS quadrant   (from sectrszscore)
    """
    enriched: dict[str, pd.DataFrame] = {}
    rs_counts: dict[str, int] = {}
    sect_counts: dict[str, int] = {}

    for ticker, df in symbol_frames.items():
        out = classify_rs_regime(
            df, rs_col="rszscore", regime_col="rsregime", ma_window=ma_window,
        )
        out = classify_rs_regime(
            out, rs_col="sectrszscore", regime_col="sectrsregime", ma_window=ma_window,
        )
        enriched[ticker] = out

        if not out.empty:
            last_rs = str(out["rsregime"].iloc[-1])
            last_sect = str(out["sectrsregime"].iloc[-1])
            rs_counts[last_rs] = rs_counts.get(last_rs, 0) + 1
            sect_counts[last_sect] = sect_counts.get(last_sect, 0) + 1

    logger.info(
        "enrich_rs_regimes: symbols=%d last-row rsregime=%s",
        len(enriched), rs_counts,
    )
    logger.info("enrich_rs_regimes: last-row sectrsregime=%s", sect_counts)
    return enriched